#!/usr/bin/env python3
import argparse
import multiprocessing
import parsl
import subscripts.config
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
from subscripts.s1_dti_preproc import run_s1
from subscripts.s2a_bedpostx import run_s2a
from subscripts.s2b_freesurfer import run_s2b
from subscripts.s3_probtrackx import run_s3
from subscripts.s4_edi import run_s4

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list', help='Text file list of subject directories.')
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject. Should not point to a parallel file system.')
parser.add_argument('--steps', type=str.lower, help='Steps to run with this script', default="s1 s2a s2b s3 s4", nargs='+')
parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', default="s2a", nargs='+')
parser.add_argument('--force', help='Force re-compute if checkpoints already exist', action='store_true')
parser.add_argument('--edge_list', help='Edges processed by s3_probtrackx and s4_edi', default=join("lists","list_edges_all.txt"))
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--group', help='Unix group to assign file permissions', default='tbidata')
parser.add_argument('--container_path', help='Path to Singularity container image')
parser.add_argument('--parsl_path', help='Path to Parsl binaries, if not installed in /usr/bin or /usr/sbin')
parser.add_argument('--username', help='Unix username for Parsl job requests', default=getpass.getuser())
parser.add_argument('--gssapi', help='Use Kerberos GSS-API authentication.', action='store_true')
parser.add_argument('--local_host_only', help='Request all jobs on local machine, ignoring other hostnames.', action='store_true')

# Site-specific machine settings
parser.add_argument('--s1_job_time', help='Average time to finish s1 on a single subject with a single node', default="00:05:00")
parser.add_argument('--s2a_job_time', help='Average time to finish s2a on a single subject with a single node', default="00:60:00")
parser.add_argument('--s2b_job_time', help='Average time to finish s2b on a single subject with a single node', default="08:00:00")
parser.add_argument('--s3_job_time', help='Average time to finish s3 on a single subject with a single node', default="12:00:00")
parser.add_argument('--s4_job_time', help='Average time to finish s4 on a single subject with a single node', default="00:45:00")
parser.add_argument('--s1_hostname', help='Hostname of machine to run step s1_dti_preproc', default='quartz.llnl.gov')
parser.add_argument('--s2a_hostname', help='Hostname of machine to run step s2a_bedpostx', default='pascal.llnl.gov')
parser.add_argument('--s2b_hostname', help='Hostname of machine to run step s2b_freesurfer', default='quartz.llnl.gov')
parser.add_argument('--s3_hostname', help='Hostname of machine to run step s3_probtrackx', default='quartz.llnl.gov')
parser.add_argument('--s4_hostname', help='Hostname of machine to run step s4_edi', default='quartz.llnl.gov')

# Override auto-generated machine settings
parser.add_argument('--s1_walltime', help='Override total walltime for step s1.')
parser.add_argument('--s2a_walltime', help='Override total walltime for step s2a.')
parser.add_argument('--s2b_walltime', help='Override total walltime for step s2b.')
parser.add_argument('--s3_walltime', help='Override total walltime for step s3.')
parser.add_argument('--s4_walltime', help='Override total walltime for step s4.')
parser.add_argument('--s1_nodes', help='Override recommended node count for step s1.')
parser.add_argument('--s2a_nodes', help='Override recommended node count for step s2a.')
parser.add_argument('--s2b_nodes', help='Override recommended node count for step s2b.')
parser.add_argument('--s3_nodes', help='Override recommended node count for step s3.')
parser.add_argument('--s4_nodes', help='Override recommended node count for step s4.')
args = parser.parse_args()

if not args.local_host_only:
    if not exists('/usr/bin/ipengine') and not exists('/usr/sbin/ipengine'):
        raise Exception("Could not find Parsl system install. Please set --parsl_path to its install location.")

steps = args.steps
if isinstance(args.steps, str):
    steps = [x.strip() for x in args.steps.split(' ') if x]
gpu_steps = args.gpu_steps
if isinstance(args.gpu_steps, str):
    gpu_steps = [x.strip() for x in args.gpu_steps.split(' ') if x]

# Make sure input files exist for each subject, then generate checksum
subjects = []
with open(args.subject_list, 'r') as f:
    input_dirs = f.readlines()
for input_dir in input_dirs:
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
num_jobs = len(subjects)
print("In total, running {} subjects".format(num_jobs))

if len(steps) < 1:
    raise Exception("Must run at least one step")

# We assume 2 vCPUs per core
cores_per_node = int(floor(multiprocessing.cpu_count() / 2))

# Set recommended or overriden node counts
node_counts = {
    's1': max(floor(0.2 * num_jobs), 1) if args.s1_nodes is None else args.s1_nodes,
    's2a': num_jobs if args.s2a_nodes is None else args.s2a_nodes,
    's2b': num_jobs if args.s2b_nodes is None else args.s2b_nodes,
    's3': num_jobs * 2 if args.s3_nodes is None else args.s3_nodes,
    's4': max(floor(0.5 * num_jobs), 1) if args.s4_nodes is None else args.s4_nodes,
}

job_times = {
    's1': args.s1_job_time,
    's2a': args.s2a_job_time,
    's2b': args.s2b_job_time,
    's3': args.s3_job_time,
    's4': args.s4_job_time,
}

walltimes = {
    's1': args.s1_walltime,
    's2a': args.s2a_walltime,
    's2b': args.s2b_walltime,
    's3': args.s3_walltime,
    's4': args.s4_walltime,
}

hostnames = {
    's1': args.s1_hostname,
    's2a': args.s2a_hostname,
    's2b': args.s2b_hostname,
    's3': args.s3_hostname,
    's4': args.s4_hostname,
}

cores_per_task = {
    's1': 1,
    's2a': cores_per_node,
    's2b': cores_per_node,
    's3': 2,
    's4': 1,
}

base_options = "#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.bank)

if args.parsl_path is not None:
    base_options += "PATH=\"{}:$PATH\"\nexport PATH\n".format(args.parsl_path)

executors = [IPyParallelExecutor(label='head',
             provider=LocalProvider(
             init_blocks=cores_per_node,
             max_blocks=cores_per_node))]

for step in steps:
    node_count = int(node_counts[step])
    print("Requesting {} {} nodes".format(node_count, step))
    if walltimes[step] is not None:
        walltime = walltimes[step]
    else:
        job_time = get_time_seconds(job_times[step])
        walltime = get_time_string(clamp((num_jobs * job_time) / node_count, 3600, 86399)) # clamp between 1 and 24 hours
    options = base_options
    if step in gpu_steps:
        options += "module load cuda/8.0;\n"
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
        workers_per_node=int(cores_per_node / cores_per_task[step]),
        provider=SlurmProvider(
            'pbatch',
            channel=channel,
            launcher=SrunLauncher(),
            nodes_per_block=node_count,
            init_blocks=1,
            max_blocks=1,
            walltime=walltime,
            scheduler_options=options,
            move_files=False,
            ),
        controller=Controller(public_ip=address_by_route()),
        )
    )

subscripts.config.s1_executor_labels = ['head'] + ['s1'] * node_counts['s1']

config = Config(executors=executors)
config.retries = 5
config.checkpoint_mode = 'task_exit'
if not args.force:
    config.checkpoint_files = get_all_checkpoints()
parsl.set_stream_logger()
parsl.load(config)

step_functions = {
    's1': run_s1,
    's2a': run_s2a,
    's2b': run_s2b,
    's3': run_s3,
    's4': run_s4,
}

prereqs = {
    's1': [],
    's2a': ['s1'],
    's2b': ['s1'],
    's3': ['s2a','s2b'],
    's4': ['s3'],
}

# Print warning if prerequisite steps are not set
if len(steps) > 1:
    for step in steps:
        for prereq in prereqs[step]:
            if prereq not in steps:
                print("Warning: step {} has prerequisite steps {}".format(step, prereqs[step]))

container = abspath(args.container_path) if args.container_path else None
odir = abspath(args.output_dir)
global_timing_log = join(odir, 'global_log', 'timing.csv')
smart_mkdir(odir)
smart_mkdir(join(odir, 'global_log'))
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,use_gpu")
if islink(join(odir,"fsaverage")):
    run("unlink {}".format(join(odir,"fsaverage")))

all_jobs = []
for subject in subjects:
    input_dir = subject['input_dir']
    sname = subject['sname']
    checksum = subject['checksum']
    sdir = join(odir, sname)
    log_dir = join(sdir,'log')
    
    smart_mkdir(log_dir)
    smart_mkdir(sdir)

    subject_jobs = {}
    inputs = []
    
    for step in steps:
        stdout, idx = get_valid_filepath(join(log_dir, step + ".stdout"))
        timing_log = join(log_dir, "{}_{:02d}_timing.txt".format(step, idx))
        smart_remove(timing_log)
        params = {
            'input_dir': input_dir,
            'sdir': sdir,
            'container': container,
            'checksum': checksum,
            'edge_list': args.edge_list,
            'group': args.group,
            'global_timing_log': global_timing_log,
            'cores_per_task': cores_per_task[step],
            'use_gpu': step in gpu_steps,
            'stdout': stdout,
            'timing_log': timing_log,
            'step': step,
        }
        # Use jobs from previous steps as inputs for current step
        inputs = []
        for prereq in prereqs[step]:
            if prereq in subject_jobs:
                inputs.append(subject_jobs[prereq])
        job = step_functions[step](params, inputs)
        subject_jobs[step] = job
        all_jobs.append((params, job))
        
for job in all_jobs:
    if job[1] is None:
        continue
    params = job[0]
    sname = basename(params['input_dir'])
    step = params['step']
    try:
        job[1].result()
        print("Finished processing step {} on subject {}".format(step, sname))
    except:
        print("Error: failed to process step {} on subject {}".format(step, sname))
