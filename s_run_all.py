#!/usr/bin/env python3
import argparse, multiprocessing, parsl, getpass, socket, json, sys
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiRunLauncher
from parsl.launchers import SimpleLauncher
from parsl.addresses import address_by_hostname
from parsl.providers import LocalProvider, SlurmProvider, CobaltProvider
from parsl.channels import SSHInteractiveLoginChannel, LocalChannel, SSHChannel
from parsl.launchers import SrunLauncher
from parsl.utils import get_all_checkpoints
from parsl.executors.ipp_controller import Controller
from parsl.addresses import address_by_route
from subscripts.utilities import *
from os.path import exists, join, split, splitext, abspath, basename, islink, isdir
from os import system,mkdir,remove,environ,makedirs
from math import floor, ceil
from subscripts.s_debug import setup_debug
from subscripts.s1_dti_preproc import setup_s1
from subscripts.s2a_bedpostx import setup_s2a
from subscripts.s2b_freesurfer import setup_s2b
from subscripts.s3_probtrackx import setup_s3
from subscripts.s3_probtrackx_alt import setup_s3_alt
from subscripts.s4_edi import setup_s4
from subscripts.s5_render import setup_s5

head_node_cores = int(floor(multiprocessing.cpu_count() / 2)) # We assume 2 vCPUs per core

class ArgsObject:
    def __init__(self, **entries):
        self.__dict__.update(entries)

if len(sys.argv) == 2 and sys.argv[1] not in ['-h', '--help']:
    config_file = sys.argv[1]
    with open(config_file) as f:
        raw_args = json.load(f)
    if not (bool('subject' in raw_args) ^ bool('subject_list' in raw_args)):
        raise Exception('Config file {} must have exactly one of: subject or subject_list'.format(config_file))
    missing_args = []
    for required_arg in ['output_dir', 'scheduler_name', 'scheduler_bank', 'scheduler_partition']:
        if required_arg not in raw_args:
            missing_args.append(required_arg)
    if missing_args:
        raise Exception('Config file {} is missing required arguments: {}'.format(config_file, missing_args))
    args = ArgsObject(**raw_args)
else:
    parser = argparse.ArgumentParser(description='Generate connectome and edge density images',
        usage='%(prog)s [config_file]\n\n<< OR >>\n\nusage: %(prog)s --subject SUBJECT_DIR --output_dir OUTPUT_DIR --scheduler_name SCHEDULER --scheduler_bank BANK --scheduler_partition PARTITION\n(see optional arguments with --help)\n')
    subjects_group = parser.add_mutually_exclusive_group(required=True)
    subjects_group.add_argument('--subject', help='Input subject directory.')
    subjects_group.add_argument('--subject_list', help='Text file list of input subject directories')
    parser.add_argument('--output_dir', help='The super-directory that will contain output directories for each subject. Avoid using a Lustre file system.', required=True)
    parser.add_argument('--scheduler_name', help='Scheduler to be used for running jobs. Values slurm for LLNL, cobalt for ANL', required=True, choices=['slurm', 'cobalt'])
    parser.add_argument('--scheduler_bank', help='Scheduler scheduler_bank to charge for jobs', required=True)
    parser.add_argument('--scheduler_partition', help='Scheduler partition to assign jobs', required=True)
    parser.add_argument('--scheduler_options', help='Command to prepend to the submit script to the scheduler')
    parser.add_argument('--gpu_options', help='String to prepend to the submit blocks for GPU-enabled steps')
    parser.add_argument('--worker_init', help="Command to be run before starting a worker, such as ‘module load Anaconda; source activate env’.")
    parser.add_argument('--steps', type=str.lower, help='Steps to run with this workflow', nargs='+')
    parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', nargs='+')
    parser.add_argument('--pbtx_edge_list', help='Text file list of edges processed by s3_probtrackx and s4_edi')
    parser.add_argument('--unix_username', help='Unix username for Parsl job requests')
    parser.add_argument('--unix_group', help='Unix group to assign file permissions')
    parser.add_argument('--container_path', help='Path to Singularity container image')
    parser.add_argument('--parsl_path', help='Path to Parsl binaries, if not installed in /usr/bin or /usr/sbin')
    # parser.add_argument('--work_dir', help='Working directory to run certain functions separate from data storage (e.g. using node-local memory)')
    parser.add_argument('--render_list', help='Text file list of NIfTI outputs for s5_render (relative to each subject output directory).')
    parser.add_argument('--gssapi', help='Use Kerberos GSS-API authentication.', action='store_true')
    parser.add_argument('--force', help='Force re-compute if checkpoints already exist', action='store_true')
    parser.add_argument('--local_host_only', help='Request all jobs on local machine, ignoring other hostnames.', action='store_false')
    parser.add_argument('--pbtx_sample_count', help='Number of streamlines in s3_probtrackx')
    parser.add_argument('--pbtx_random_seed', help='Random seed in s3_probtrackx')
    parser.add_argument('--connectome_idx_list', help='Text file with pairs of volumes and connectome indices')
    parser.add_argument('--histogram_bin_count', help='Number of bins in NiFTI image histograms')
    parser.add_argument('--compress_pbtx_results', help='Compress probtrackx outputs to reduce inode and disk space usage', action='store_false')
    parser.add_argument('--fast_pbtx', help='Use 1-to-N instead of N-to-N probtrackx script', action='store_false')

    # Site-specific machine settings
    parser.add_argument('--s1_hostname', help='Hostname of machine to run step s1_dti_preproc')
    parser.add_argument('--s2a_hostname', help='Hostname of machine to run step s2a_bedpostx')
    parser.add_argument('--s2b_hostname', help='Hostname of machine to run step s2b_freesurfer')
    parser.add_argument('--s3_hostname', help='Hostname of machine to run step s3_probtrackx')
    parser.add_argument('--s4_hostname', help='Hostname of machine to run step s4_edi')
    parser.add_argument('--s5_hostname', help='Hostname of machine to run step s5_render')
    parser.add_argument('--s1_cores_per_task', help='Number of cores to assign each task for step s1_dti_preproc')
    parser.add_argument('--s2a_cores_per_task', help='Number of cores to assign each task for step s2a_bedpostx')
    parser.add_argument('--s2b_cores_per_task', help='Number of cores to assign each task for step s2b_freesurfer')
    parser.add_argument('--s3_cores_per_task', help='Number of cores to assign each task for step s3_probtrackx')
    parser.add_argument('--s4_cores_per_task', help='Number of cores to assign each task for step s4_edi')
    parser.add_argument('--s5_cores_per_task', help='Number of cores to assign each task for step s5_render')
    parser.add_argument('--s1_walltime', help='Walltime for step s1.')
    parser.add_argument('--s2a_walltime', help='Walltime for step s2a.')
    parser.add_argument('--s2b_walltime', help='Walltime for step s2b.')
    parser.add_argument('--s3_walltime', help='Walltime for step s3.')
    parser.add_argument('--s4_walltime', help='Walltime for step s4.')
    parser.add_argument('--s5_walltime', help='Walltime for step s5.')

    # Dynamic walltime
    parser.add_argument('--dynamic_walltime', help='Request dynamically shortened walltimes, to gain priority on job queue', action='store_true')
    parser.add_argument('--s1_job_time', help='If using dynamic walltime, duration of s1 on a single subject with a single node')
    parser.add_argument('--s2a_job_time', help='If using dynamic walltime, duration of s2a on a single subject with a single node')
    parser.add_argument('--s2b_job_time', help='If using dynamic walltime, duration of s2b on a single subject with a single node')
    parser.add_argument('--s3_job_time', help='If using dynamic walltime, duration of s3 on a single subject with a single node')
    parser.add_argument('--s4_job_time', help='If using dynamic walltime, duration of s4 on a single subject with a single node')
    parser.add_argument('--s5_job_time', help='If using dynamic walltime, duration of s5 on a single subject with a single node')

    # Override auto-generated machine settings
    parser.add_argument('--s1_nodes', help='Override recommended node count for step s1.')
    parser.add_argument('--s2a_nodes', help='Override recommended node count for step s2a.')
    parser.add_argument('--s2b_nodes', help='Override recommended node count for step s2b.')
    parser.add_argument('--s3_nodes', help='Override recommended node count for step s3.')
    parser.add_argument('--s4_nodes', help='Override recommended node count for step s4.')
    parser.add_argument('--s5_nodes', help='Override recommended node count for step s5.')
    parser.add_argument('--s1_cores', help='Override core count for node running step s1.')
    parser.add_argument('--s2a_cores', help='Override core count for node running step s2a.')
    parser.add_argument('--s2b_cores', help='Override core count for node running step s2b.')
    parser.add_argument('--s3_cores', help='Override core count for node running step s3.')
    parser.add_argument('--s4_cores', help='Override core count for node running step s4.')
    parser.add_argument('--s5_cores', help='Override core count for node running step s5.')
    args = parser.parse_args()

############################
# Defaults for Optional Args
############################

parse_default('steps', "s1 s2a s2b s3 s4 s5", args)
parse_default('gpu_steps', "s2a", args)
parse_default('pbtx_edge_list', join("lists","list_edges_reduced.txt"), args)
parse_default('scheduler_options', "", args)
parse_default('worker_init', "", args)
parse_default('gpu_options', " ", args)
parse_default('container_path', "container/image.simg", args)
parse_default('unix_username', getpass.getuser(), args)
parse_default('unix_group', None, args)
parse_default('force', False, args)
parse_default('gssapi', False, args)
parse_default('local_host_only', True, args)
parse_default('compress_pbtx_results', True, args)
parse_default('fast_pbtx', False, args)
# parse_default('work_dir', None, args)
parse_default('parsl_path', None, args)
parse_default('render_list', "lists/render_targets.txt", args)
parse_default('pbtx_sample_count', 200, args)
parse_default('pbtx_random_seed', None, args)
parse_default('connectome_idx_list', "lists/connectome_idxs.txt", args)
parse_default('histogram_bin_count', 256, args)
parse_default('s1_cores_per_task', 1, args)
parse_default('s2a_cores_per_task', head_node_cores, args)
parse_default('s2b_cores_per_task', head_node_cores, args)
parse_default('s3_cores_per_task', 3, args)
parse_default('s4_cores_per_task', 1, args)
parse_default('s5_cores_per_task', 1, args)
parse_default('s5_cores_per_task', 1, args)
parse_default('s1_walltime', "23:59:00", args)
parse_default('s2a_walltime', "23:59:00", args)
parse_default('s2b_walltime', "23:59:00", args)
parse_default('s3_walltime', "23:59:00", args)
parse_default('s4_walltime', "23:59:00", args)
parse_default('s5_walltime', "23:59:00", args)
parse_default('dynamic_walltime', False, args)
parse_default('s1_job_time', "00:15:00", args)
parse_default('s2a_job_time', "00:45:00", args)
parse_default('s2b_job_time', "10:00:00", args)
parse_default('s3_job_time', "23:59:00", args)
parse_default('s4_job_time', "00:45:00", args)
parse_default('s5_job_time', "00:15:00", args)

for step in ['s1','s2a','s2b','s3','s4','s5']:
    parse_default(step + '_hostname', None, args)
    parse_default(step + '_nodes', None, args)
    parse_default(step + '_cores', None, args)

############################
# Steps
############################

print("\n===================================================")
print("Setup")
print("---------------------------------------------------")
steps = args.steps if args.steps else []
gpu_steps = args.gpu_steps if args.gpu_steps else []
if isinstance(args.steps, str):
    steps = [x.strip() for x in args.steps.split(' ') if x]
if isinstance(args.gpu_steps, str):
    gpu_steps = [x.strip() for x in args.gpu_steps.split(' ') if x]
valid_steps = ['debug','s1','s2a','s2b','s3','s4','s5']
steps = [x for x in steps if x in valid_steps]
gpu_steps = [x for x in gpu_steps if x in valid_steps]
if len(steps) != len(set(steps)):
    raise Exception("Argument \"steps\" has duplicate values")
if len(gpu_steps) != len(set(gpu_steps)):
    raise Exception("Argument \"gpu_steps\" has duplicate values")
step_setup_functions = {
    'debug': setup_debug,
    's1': setup_s1,
    's2a': setup_s2a,
    's2b': setup_s2b,
    's3': setup_s3 if args.fast_pbtx else setup_s3_alt,
    's4': setup_s4,
    's5': setup_s5,
}
prereqs = {
    'debug': [],
    's1': [],
    's2a': ['s1'],
    's2b': ['s1'],
    's3': ['s1','s2a','s2b'],
    's4': ['s1','s2a','s2b','s3'],
    's5': ['s1','s2a','s2b','s3','s4'],
}
# Print warning if prerequisite steps are not running
for step in steps:
    for prereq in prereqs[step]:
        if prereq not in steps:
            print("Warning: step {} has prerequisite steps {}. You are only running steps {}.".format(step, prereqs[step], steps))
            break

############################
# Inputs
############################

odir = abspath(args.output_dir)
container = abspath(args.container_path) if args.container_path else None
global_timing_log = join(odir, 'global_timing', 'timing.csv')
smart_mkdir(odir)
smart_mkdir(join(odir, 'global_timing'))
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,use_gpu")
if islink(join(odir,"fsaverage")):
    run("unlink {}".format(join(odir,"fsaverage")))
pbtx_edge_list = abspath(args.pbtx_edge_list)
render_list = abspath(args.render_list)
connectome_idx_list = abspath(args.connectome_idx_list)

if hasattr(args, 'subject_list') and args.subject_list is not None:
    input_dirs = open(args.subject_list, 'r').readlines()
else:
    input_dirs = [args.subject]
subject_dict = {}



for input_dir in input_dirs:
    # Make sure input files exist for each subject
    input_dir = abspath(input_dir.strip())
    if not input_dir or not isdir(input_dir):
        print("Invalid input dir {}".format(input_dir))
        continue
    checksum = None
    sname = basename(input_dir)
    if 's1' in steps:


        bvecs = join(input_dir, "bvecs")
        bvals = join(input_dir, "bvals")
        hardi = join(input_dir, "hardi.nii.gz")
        anat = join(input_dir, "anat.nii.gz")

        data = join(input_dir, "data.nii.gz")
        if not exists(hardi) and exists(data):
            smart_copy(data, hardi)

        T1 = join(input_dir, "T1.nii.gz")
        if not exists(anat) and exists(T1):
            smart_copy(T1, anat)

        if not exist_all([bvecs, bvals, hardi, anat]):
            print("Skipping subject {}. Missing input files - must have bvecs, bvals, hardi.nii.gz, and anat.nii.gz.".format(sname))
            continue
        checksum = generate_checksum(input_dir) # Ensures recompute if inputs change

    sdir = join(odir, sname)
    log_dir = join(sdir,'log')
    # if args.work_dir:
    #     work_sdir = join(args.work_dir, sname)
    # else:
    #     work_sdir = None

    smart_mkdir(log_dir)
    smart_mkdir(sdir)
    subject = {}
    for step in steps:
        params = {
            'input_dir': input_dir,
            'sdir': sdir,
            'container': container,
            'checksum': checksum,
            'pbtx_edge_list': pbtx_edge_list,
            'group': args.unix_group,
            'global_timing_log': global_timing_log,
            'use_gpu': step in gpu_steps,
            'step': step,
            # 'work_sdir': work_sdir,
            'render_list': render_list,
            'connectome_idx_list': connectome_idx_list,
            'pbtx_sample_count': int(args.pbtx_sample_count),
            'pbtx_random_seed': args.pbtx_random_seed,
            'histogram_bin_count': int(args.histogram_bin_count),
            'compress_pbtx_results': args.compress_pbtx_results,
        }
        stdout_template = join(log_dir, "{}.stdout".format(step))
        new_stdout, prev_stdout, idx = get_log_path(stdout_template)
        params_log = join(log_dir, step + "_params.txt")
        if exists(params_log) and not args.force:
            with open(params_log) as f:
                old_params = json.load(f)
                for k in params:
                    if k not in old_params or str(params[k]) != str(old_params[k]):
                        break
                else:
                    if is_log_complete(prev_stdout):
                        print("Skipping step \"{}\" for subject {} after finding completed log. Use --force to rerun.".format(step, sname))
                        continue
                    else:
                        if exists(prev_stdout):
                            new_stdout = prev_stdout
                            idx -= 1
        timing_log = join(log_dir, "{}_{:02d}_timing.txt".format(step, idx))
        smart_remove(params_log)
        smart_remove(timing_log)
        with open(params_log, 'w') as f:
            json.dump(params, f)
        params['stdout'] = new_stdout
        params['timing_log'] = timing_log

        subject[step] = params
    subject_dict[sname] = subject
    running_steps = list(subject.keys())
    if len(running_steps) > 0:
        print("Running subject {} with steps {}".format(sname, running_steps))
    else:
        print("Not running any steps for subject {}".format(sname))
num_subjects = {
    'debug': 0,
    's1': 0,
    's2a': 0,
    's2b': 0,
    's3': 0,
    's4': 0,
    's5': 0,
}
for sname in subject_dict:
    for step in subject_dict[sname]:
        num_subjects[step] += 1
total_num_steps = sum(num_subjects.values())
if total_num_steps == 0:
    print("Error: not running any steps on any subjects")
    exit(0)
else:
    print("In total, running {} steps across {} subjects".format(total_num_steps, len(subject_dict)))

# if len(subject_dict) > 100:
    # raise Exception("Workflow is unstable for large numbers of subjects. Please specify fewer than 100.")

############################
# Node Settings
############################

def get_walltime(_num_subjects, job_time_string, node_count):
    job_time = get_time_seconds(job_time_string)
    return get_time_string(clamp((_num_subjects * job_time) / node_count, 7200, 86399)) # clamp between 2 and 24 hours

# Set recommended or overriden node counts
node_counts = {
    'debug': 1,
    's1': max(floor(0.2 * num_subjects['s1']), 1) if args.s1_nodes is None else int(args.s1_nodes),
    's2a': max(floor(1.0 * num_subjects['s2a']), 1) if args.s2a_nodes is None else int(args.s2a_nodes),
    's2b': max(floor(1.0 * num_subjects['s2b']), 1) if args.s2b_nodes is None else int(args.s2b_nodes),
    's3': max(floor(1.0 * num_subjects['s3']), 1) if args.s3_nodes is None else int(args.s3_nodes),
    's4': max(floor(0.1 * num_subjects['s4']), 1) if args.s4_nodes is None else int(args.s4_nodes),
    's5': max(floor(0.1 * num_subjects['s5']), 1) if args.s5_nodes is None else int(args.s5_nodes),
}
job_times = {
    's1': args.s1_job_time,
    's2a': args.s2a_job_time,
    's2b': args.s2b_job_time,
    's3': args.s3_job_time,
    's4': args.s4_job_time,
    's5': args.s5_job_time,
}
walltimes = {
    'debug': "00:05:00",
    's1': get_walltime(num_subjects['s1'], job_times['s1'], node_counts['s1']) if args.dynamic_walltime else args.s1_walltime,
    's2a': get_walltime(num_subjects['s2a'], job_times['s2a'], node_counts['s2a']) if args.dynamic_walltime else args.s2a_walltime,
    's2b': get_walltime(num_subjects['s2b'], job_times['s2b'], node_counts['s2b']) if args.dynamic_walltime else args.s2b_walltime,
    's3': get_walltime(num_subjects['s3'], job_times['s3'], node_counts['s3']) if args.dynamic_walltime else args.s3_walltime,
    's4': get_walltime(num_subjects['s4'], job_times['s4'], node_counts['s4']) if args.dynamic_walltime else args.s4_walltime,
    's5': get_walltime(num_subjects['s5'], job_times['s5'], node_counts['s5']) if args.dynamic_walltime else args.s5_walltime,
}
hostnames = {
    'debug': args.s1_hostname,
    's1': args.s1_hostname,
    's2a': args.s2a_hostname,
    's2b': args.s2b_hostname,
    's3': args.s3_hostname,
    's4': args.s4_hostname,
    's5': args.s5_hostname,
}
cores_per_node = {
    'debug': head_node_cores,
    's1': head_node_cores if args.s1_cores is None else int(args.s1_cores),
    's2a': head_node_cores if args.s2a_cores is None else int(args.s2a_cores),
    's2b': head_node_cores if args.s2b_cores is None else int(args.s2b_cores),
    's3': head_node_cores if args.s3_cores is None else int(args.s3_cores),
    's4': head_node_cores if args.s4_cores is None else int(args.s4_cores),
    's5': head_node_cores if args.s5_cores is None else int(args.s5_cores),
}
cores_per_task = {
    'debug': 1,
    's1': args.s1_cores_per_task,
    's2a': args.s2a_cores_per_task,
    's2b': args.s2b_cores_per_task,
    's3': args.s3_cores_per_task, # setting this too low can lead to probtrackx exceeding local memory and crashing
    's4': args.s4_cores_per_task,
    's5': args.s5_cores_per_task,
}

############################
# Parsl
############################

if not args.local_host_only:
    if not exists('/usr/bin/ipengine') and not exists('/usr/sbin/ipengine'):
        raise Exception("Could not find Parsl system install. Please set --parsl_path to its install location.")

base_options = ""
if args.scheduler_name == 'slurm':
    base_options += "#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.scheduler_bank)
base_options += str(args.scheduler_options) + '\n'
if args.parsl_path is not None:
    base_options += "PATH=\"{}:$PATH\"\nexport PATH\n".format(args.parsl_path)

executors = []
for step in steps:
    node_count = int(node_counts[step])
    print("Requesting {} nodes for step \"{}\"".format(node_count, step))
    options = base_options
    if step in gpu_steps:
        options += str(args.gpu_options) + '\n'
    if args.local_host_only:
        channel = LocalChannel()
    else:
        if hostnames[step] is None:
            raise Exception('To run step {} on a remote host, please set the argument --{}_hostname'.format(step))
        channel = SSHChannel(
                hostname=hostnames[step],
                username=args.unix_username,
                gssapi_auth=args.gssapi,
                )
    if args.scheduler_name == 'slurm':
        executors.append(IPyParallelExecutor(
                    label=step,
                    workers_per_node=int(int(cores_per_node[step]) / int(cores_per_task[step])),
                    provider=SlurmProvider(
                        args.scheduler_partition,
                        channel=channel,
                        launcher=SrunLauncher(),
                        nodes_per_block=node_count,
                        worker_init=args.worker_init,
                        init_blocks=1,
                        max_blocks=1,
                        walltime=walltimes[step],
                        scheduler_options=options,
                        move_files=False,
                        ),
                    controller=Controller(public_ip=address_by_route()),
                    )
                )
    else:
        executors.append(HighThroughputExecutor(
                    label=step,
                    worker_debug=False,
                    address=address_by_hostname(),
                    provider=CobaltProvider(
                        queue=args.scheduler_partition,
                        account=args.scheduler_bank, # project name to submit the job
                        # account='CSC249ADOA01',
                        channel=channel,
                        launcher=SimpleLauncher(),
                        scheduler_options=options,  # string to prepend to #COBALT blocks in the submit script to the scheduler
                        worker_init=args.worker_init, # command to run before starting a worker, such as 'source activate env'
                        # worker_init='source /home/madduri/setup_cooley_env.sh',
                        init_blocks=1,
                        max_blocks=1,
                        nodes_per_block=node_count,
                        walltime=walltimes[step],
                        ),
                    )
                )
print("===================================================\n")

config = Config(executors=executors)
config.retries = 5
config.checkpoint_mode = 'task_exit'
if not args.force:
    config.checkpoint_files = get_all_checkpoints()
parsl.set_stream_logger()
parsl.load(config)

############################
# Outputs
############################

all_jobs = []
for sname in subject_dict:
    subject_jobs = {} # Store jobs in previous steps to use as inputs
    for step in subject_dict[sname]:
        params = subject_dict[sname][step]
        params['cores_per_task'] = int(cores_per_task[step])
        inputs = []
        actual_prereqs = []
        for prereq in prereqs[step]:
            if prereq in subject_jobs:
                inputs.append(subject_jobs[prereq])
                actual_prereqs.append(prereq)
        print("Starting step {} after prereq steps {}".format(step, actual_prereqs))
        job_future = step_setup_functions[step](params, inputs)
        subject_jobs[step] = job_future
        all_jobs.append((params, job_future))
        
for job in all_jobs:
    params = job[0]
    job_future = job[1]
    if job_future is None:
        continue
    sname = basename(params['input_dir'])
    step = params['step']
    try:
        job_future.result()
        print("Finished processing step \"{}\" for subject {}".format(step, sname))
    except:
        print("Error: failed to process step \"{}\" for subject {}".format(step, sname))
    
