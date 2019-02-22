#!/usr/bin/env python3
import argparse
import multiprocessing
import parsl
import getpass
import socket
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.providers import LocalProvider,SlurmProvider
from parsl.channels import SSHInteractiveLoginChannel, LocalChannel, SSHChannel
from parsl.launchers import SrunLauncher
from parsl.utils import get_all_checkpoints
from parsl.executors.ipp_controller import Controller
from parsl.addresses import address_by_route
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,islink,isdir
from os import system,mkdir,remove,environ,makedirs
from math import floor, ceil
from subscripts.s_debug import setup_debug
from subscripts.s1_dti_preproc import setup_s1
from subscripts.s2a_bedpostx import setup_s2a
from subscripts.s2b_freesurfer import setup_s2b
from subscripts.s3_probtrackx import setup_s3
from subscripts.s4_edi import setup_s4
from subscripts.s5_render import setup_s5

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list', help='Text file list of subject directories.')
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject. Avoid using a Lustre file system.')
parser.add_argument('--steps', type=str.lower, help='Steps to run with this script', default="s1 s2a s2b s3 s4 s5", nargs='+')
parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', default="s2a", nargs='+')
parser.add_argument('--force', help='Force re-compute if checkpoints already exist', action='store_true')
parser.add_argument('--edge_list', help='Text file list of edges processed by s3_probtrackx and s4_edi', default=join("lists","list_edges_reduced.txt"))
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--partition', help='Slurm partition to assign jobs', default="pbatch")
parser.add_argument('--scheduler_options', help='String to append to the #SBATCH blocks in the submit script to the scheduler')
parser.add_argument('--gpu_options', help='String to append to the #SBATCH blocks for GPU-enabled steps', default="module load cuda/8.0;")
parser.add_argument('--group', help='Unix group to assign file permissions')
parser.add_argument('--container_path', help='Path to Singularity container image')
parser.add_argument('--parsl_path', help='Path to Parsl binaries, if not installed in /usr/bin or /usr/sbin')
parser.add_argument('--username', help='Unix username for Parsl job requests', default=getpass.getuser())
parser.add_argument('--gssapi', help='Use Kerberos GSS-API authentication.', action='store_true')
parser.add_argument('--local_host_only', help='Request all jobs on local machine, ignoring other hostnames.', action='store_true')
parser.add_argument('--work_dir', help='Working directory to run certain functions separate from data storage (e.g. using node-local memory)')
parser.add_argument('--render_list', help='Text file list of NIfTI outputs for s5_render (relative to each subject output directory).', default='lists/render_targets.txt')
parser.add_argument('--fast_probtrackx', help='Faster parameters for s3_probtrackx. 1/5th number of streamlines, 1/2 number of steps per streamline', action='store_true')
parser.add_argument('--histogram_bin_count', help='Number of bins in NiFTI image histograms', default=256)

# Site-specific machine settings
parser.add_argument('--s1_job_time', help='Average time to finish s1 on a single subject with a single node', default="00:15:00")
parser.add_argument('--s2a_job_time', help='Average time to finish s2a on a single subject with a single node', default="00:45:00")
parser.add_argument('--s2b_job_time', help='Average time to finish s2b on a single subject with a single node', default="08:00:00")
parser.add_argument('--s3_job_time', help='Average time to finish s3 on a single subject with a single node', default="12:00:00")
parser.add_argument('--s4_job_time', help='Average time to finish s4 on a single subject with a single node', default="00:45:00")
parser.add_argument('--s5_job_time', help='Average time to finish s5 on a single subject with a single node', default="00:05:00")
parser.add_argument('--s1_hostname', help='Hostname of machine to run step s1_dti_preproc', default='quartz.llnl.gov')
parser.add_argument('--s2a_hostname', help='Hostname of machine to run step s2a_bedpostx', default='pascal.llnl.gov')
parser.add_argument('--s2b_hostname', help='Hostname of machine to run step s2b_freesurfer', default='quartz.llnl.gov')
parser.add_argument('--s3_hostname', help='Hostname of machine to run step s3_probtrackx', default='quartz.llnl.gov')
parser.add_argument('--s4_hostname', help='Hostname of machine to run step s4_edi', default='quartz.llnl.gov')
parser.add_argument('--s5_hostname', help='Hostname of machine to run step s5_render', default='quartz.llnl.gov')

# Override auto-generated machine settings
parser.add_argument('--s1_walltime', help='Override total walltime for step s1.')
parser.add_argument('--s2a_walltime', help='Override total walltime for step s2a.')
parser.add_argument('--s2b_walltime', help='Override total walltime for step s2b.')
parser.add_argument('--s3_walltime', help='Override total walltime for step s3.')
parser.add_argument('--s4_walltime', help='Override total walltime for step s4.')
parser.add_argument('--s5_walltime', help='Override total walltime for step s5.')
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
# Steps
############################

steps = args.steps
gpu_steps = args.gpu_steps
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
    's3': setup_s3,
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

# Make sure input files exist for each subject, then generate checksum
subjects = []
for input_dir in open(args.subject_list, 'r').readlines():
    input_dir = input_dir.strip()
    if not input_dir or not isdir(input_dir):
        continue
    bvecs = join(input_dir, "bvecs")
    bvals = join(input_dir, "bvals")
    hardi = join(input_dir, "hardi.nii.gz")
    anat = join(input_dir, "anat.nii.gz")
    sname = basename(input_dir)
    if not exist_all([bvecs, bvals, hardi, anat]):
        print("Skipping subject {}. Missing input files - must have bvecs, bvals, hardi.nii.gz, and anat.nii.gz.".format(sname))
        continue
    checksum = generate_checksum(input_dir)
    subjects.append({'input_dir':input_dir, 'sname':sname, 'checksum':checksum})
    print("Running subject {} with steps {}".format(sname, steps))
num_subjects = len(subjects)
if num_subjects == 0:
    raise Exception("Not running any subjects")
print("In total, running {} subjects".format(num_subjects))


############################
# Node Settings
############################

def get_walltime(num_subjects, job_time_string, node_count):
    job_time = get_time_seconds(job_time_string)
    return get_time_string(clamp((num_subjects * job_time) / node_count, 7200, 86399)) # clamp between 2 and 24 hours

# We assume 2 vCPUs per core
head_node_cores = int(floor(multiprocessing.cpu_count() / 2))

# Set recommended or overriden node counts
node_counts = {
    'debug': 1,
    's1': max(floor(0.2 * num_subjects), 1) if args.s1_nodes is None else int(args.s1_nodes),
    's2a': max(floor(1.0 * num_subjects), 1) if args.s2a_nodes is None else int(args.s2a_nodes),
    's2b': max(floor(1.0 * num_subjects), 1) if args.s2b_nodes is None else int(args.s2b_nodes),
    's3': max(floor(2.0 * num_subjects), 1) if args.s3_nodes is None else int(args.s3_nodes),
    's4': max(floor(0.1 * num_subjects), 1) if args.s4_nodes is None else int(args.s4_nodes),
    's5': max(floor(0.1 * num_subjects), 1) if args.s5_nodes is None else int(args.s5_nodes),
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
    's1': get_walltime(num_subjects, job_times['s1'], node_counts['s1']) if args.s1_walltime is None else args.s1_walltime,
    's2a': get_walltime(num_subjects, job_times['s2a'], node_counts['s2a']) if args.s2a_walltime is None else args.s2a_walltime,
    's2b': get_walltime(num_subjects, job_times['s2b'], node_counts['s2b']) if args.s2b_walltime is None else args.s2b_walltime,
    's3': get_walltime(num_subjects, job_times['s3'], node_counts['s3']) if args.s3_walltime is None else args.s3_walltime,
    's4': get_walltime(num_subjects, job_times['s4'], node_counts['s4']) if args.s4_walltime is None else args.s4_walltime,
    's5': get_walltime(num_subjects, job_times['s5'], node_counts['s5']) if args.s5_walltime is None else args.s5_walltime,
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
    's1': 1,
    's2a': cores_per_node['s2a'],
    's2b': cores_per_node['s2b'],
    's3': 3, # setting this too low can lead to probtrackx exceeding local memory and crashing
    's4': 1,
    's5': 1,
}

############################
# Parsl
############################

if not args.local_host_only:
    if not exists('/usr/bin/ipengine') and not exists('/usr/sbin/ipengine'):
        raise Exception("Could not find Parsl system install. Please set --parsl_path to its install location.")

executors = [IPyParallelExecutor(label='head',
             provider=LocalProvider(
             init_blocks=head_node_cores,
             max_blocks=head_node_cores))]

base_options = "#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.bank)
if args.scheduler_options is not None:
    base_options += str(args.scheduler_options) + '\n'

if args.parsl_path is not None:
    base_options += "PATH=\"{}:$PATH\"\nexport PATH\n".format(args.parsl_path)

for step in steps:
    node_count = int(node_counts[step])
    print("Requesting {} {} nodes".format(node_count, step))
    options = base_options
    if step in gpu_steps:
        options += str(args.gpu_options) + '\n'
    if args.local_host_only:
        channel = LocalChannel()
    else:
        channel = SSHChannel(
                hostname=hostnames[step],
                username=args.username,
                gssapi_auth=args.gssapi,
                )
    executors.append(IPyParallelExecutor(
        label=step,
        workers_per_node=int(cores_per_node[step] / cores_per_task[step]),
        provider=SlurmProvider(
            args.partition,
            channel=channel,
            launcher=SrunLauncher(),
            nodes_per_block=node_count,
            init_blocks=1,
            max_blocks=1,
            walltime=walltimes[step],
            scheduler_options=options,
            move_files=False,
            ),
        controller=Controller(public_ip=address_by_route()),
        )
    )

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

container = abspath(args.container_path) if args.container_path else None
odir = abspath(args.output_dir)
global_timing_log = join(odir, 'global_timing', 'timing.csv')
smart_mkdir(odir)
smart_mkdir(join(odir, 'global_timing'))
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,use_gpu")
if islink(join(odir,"fsaverage")):
    run("unlink {}".format(join(odir,"fsaverage")))
edge_list = abspath(args.edge_list)
render_list = abspath(args.render_list)

all_jobs = []
for subject in subjects:
    input_dir = subject['input_dir']
    sname = subject['sname']
    checksum = subject['checksum']
    sdir = join(odir, sname)
    log_dir = join(sdir,'log')
    if args.work_dir:
        work_sdir = join(args.work_dir, sname)
    else:
        work_sdir = None
    
    smart_mkdir(log_dir)
    smart_mkdir(sdir)

    subject_jobs = {} # Store jobs in previous steps to use as inputs
    
    for step in steps:
        log_template = join(log_dir, step + ".stdout")
        new_log, prev_log, idx = get_log_path(log_template)
        if is_log_complete(prev_log):
            if not args.force:
                print("Skipping step {} for subject {} after finding completed log. Use --force to rerun.".format(step, sname))
                continue
            stdout = new_log
        else:
            # Use last log output if it hasn't completed (to preserve Parsl checkpointing)
            if exists(prev_log):
                stdout = prev_log
                idx -= 1
            else:
                stdout = new_log

        timing_log = join(log_dir, "{}_{:02d}_timing.txt".format(step, idx))
        smart_remove(timing_log)
        params = {
            'input_dir': input_dir,
            'sdir': sdir,
            'container': container,
            'checksum': checksum,
            'edge_list': edge_list,
            'group': args.group,
            'global_timing_log': global_timing_log,
            'cores_per_task': cores_per_task[step],
            'use_gpu': step in gpu_steps,
            'stdout': stdout,
            'timing_log': timing_log,
            'step': step,
            'work_sdir': work_sdir,
            'render_list': render_list,
            'fast_probtrackx': args.fast_probtrackx,
            'histogram_bin_count': int(args.histogram_bin_count),
        }

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
        print("Finished processing step {} on subject {}".format(step, sname))
    except:
        print("Error: failed to process step {} on subject {}".format(step, sname))
    
