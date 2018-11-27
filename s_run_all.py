#!/usr/bin/env python3
import argparse
import multiprocessing
import parsl
import subscripts.config
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.providers import LocalProvider,SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.utils import get_all_checkpoints
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
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject')
parser.add_argument('--steps', type=str.lower, help='Steps to run with this script', default="s1 s2a s2b s3 s4", nargs='+')
parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', default="s2a", nargs='+')
node_count = parser.add_mutually_exclusive_group()
node_count.add_argument('--max_nodes', help='Max number of nodes to request. If not set, will prompt for user input.')
# node_count.add_argument('--use_recommended_nodes', help='Use recommended number of nodes, disabling prompt.',action='store_true')
parser.add_argument('--force', help='Force re-compute if checkpoints already exist',action='store_true')
parser.add_argument('--edge_list', help='Edges processed by s3_probtrackx and s4_edi', default=join("lists","listEdgesEDI.txt"))
parser.add_argument('--s1_job_time', help='Average time to finish s1 on a single subject with a single node', default="00:05:00")
parser.add_argument('--s2_job_time', help='Average time to finish s2a and s2b on a single subject with a single node', default="08:00:00")
parser.add_argument('--s3_job_time', help='Average time to finish s3 on a single subject with a single node', default="06:00:00")
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--group', help='Unix group to assign file permissions', default='tbidata')
parser.add_argument('--local_only', help='Run only on local machine', action='store_true')
parser.add_argument('--container', help='Path to Singularity container image')
parser.add_argument('--one_core_walltime', help='Override walltime for one-core executor. For steps s1_dti_preproc and s4_edi.')
parser.add_argument('--two_core_walltime', help='Override walltime for two-core executor. For step s3_probtrackx.')
parser.add_argument('--all_core_walltime', help='Override walltime for all-core executor. For steps s2a_bedpostx and s2b_freesurfer.')
parser.add_argument('--one_core_nodes', help='Override max_nodes setting. Number of nodes with one core per task. For steps s1_dti_preproc and s4_edi.')
parser.add_argument('--two_core_nodes', help='Override max_nodes setting. Number of nodes with two cores per task. For step s3_probtrackx.')
parser.add_argument('--all_core_nodes', help='Override max_nodes setting. Number of nodes using all cores on each task. For steps s2a_bedpostx and s2b_freesurfer.')
args = parser.parse_args()

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

# Weight core allocations, to determine recommended nodes and allocate nodes of each type
# Weight values are somewhat arbitrary, based on core-time consumption in testing
one_core_weight = 0.1 if running_step(steps, 's1', 's4') else 0
two_core_weight = 0.4 if running_step(steps, 's3') else 0
all_core_weight = 0.5 if running_step(steps, 's2a', 's2b') else 0
total_weight = one_core_weight + two_core_weight + all_core_weight

# Minimum 1 node of each type, if required by any steps
one_core_min = 1 if running_step(steps, 's1', 's4') else 0
two_core_min = 1 if running_step(steps, 's3') else 0
all_core_min = 1 if running_step(steps, 's2a', 's2b') else 0
total_min_nodes = one_core_min + two_core_min + all_core_min

if args.max_nodes is not None:
    max_nodes = int(args.max_nodes)
else:
    recommended_nodes = max(int(ceil(3 * total_weight * num_jobs)), total_min_nodes)
    max_nodes = recommended_nodes
    # if args.use_recommended_nodes:
    #     max_nodes = recommended_nodes
    # else:
    #     question = "How many nodes to request? [Recommended: {}] ".format(recommended_nodes)
    #     while 1:
    #         sys.stdout.write(question)
    #         choice = input().lower()
    #         if choice == '':
    #             max_nodes = recommended_nodes
    #             break
    #         elif is_integer(choice):
    #             if int(choice) < total_min_nodes:
    #                 sys.stdout.write("Job requires at least {} nodes\n".format(total_min_nodes))
    #             else:
    #                 max_nodes = int(choice)
    #                 break
    #         else:
    #             sys.stdout.write("Please respond with an integer value\n")

if max_nodes < total_min_nodes:
    raise Exception("Job requires at least {} nodes".format(total_min_nodes))

# Calculate number of nodes of each type
one_core_nodes = max(int(floor((one_core_weight / total_weight) * max_nodes)), one_core_min)
two_core_nodes = max(int(floor((two_core_weight / total_weight) * max_nodes)), two_core_min)
all_core_nodes = max(max_nodes - two_core_nodes - one_core_nodes, all_core_min)

# Override number of nodes using command line arguments
one_core_nodes = int(args.one_core_nodes) if args.one_core_nodes is not None else one_core_nodes
two_core_nodes = int(args.two_core_nodes) if args.two_core_nodes is not None else two_core_nodes
all_core_nodes = int(args.all_core_nodes) if args.all_core_nodes is not None else all_core_nodes
if args.local_only:
    one_core_nodes = 0
    two_core_nodes = 0
    all_core_nodes = 0
    if running_step(steps, 's2a', 's2b', 's3'):
        raise Exception("Steps s2a, s2b, and s3 cannot by run with just the local node.")

print("Running with {} max nodes".format(one_core_nodes + two_core_nodes + all_core_nodes))

s1_job_time = get_time_seconds(args.s1_job_time)
s2_job_time = get_time_seconds(args.s2_job_time)
s3_job_time = get_time_seconds(args.s3_job_time)

# We assume 2 vCPUs per core
cores_per_node = int(floor(multiprocessing.cpu_count() / 2))

overrides = "#SBATCH -A {}".format(args.bank)

executors = [IPyParallelExecutor(label='one_core_head',
             provider=LocalProvider(
             init_blocks=cores_per_node,
             max_blocks=cores_per_node))]
if one_core_nodes > 0:
    one_core_walltime = get_time_string(clamp((num_jobs * s1_job_time) / one_core_nodes, 3600, 86399)) # clamp between 1 and 24 hours
    one_core_walltime = args.one_core_walltime if args.one_core_walltime is not None else one_core_walltime
    executors.append(IPyParallelExecutor(label='one_core',
                     provider=SlurmProvider('pbatch',
                     launcher=SrunLauncher(),
                     nodes_per_block=one_core_nodes,
                     tasks_per_node=cores_per_node,
                     init_blocks=1,
                     max_blocks=1,
                     walltime=one_core_walltime,
                     overrides=overrides)))
if all_core_nodes > 0:
    all_core_walltime = get_time_string(clamp((num_jobs * s2_job_time) / all_core_nodes, 21600, 86399)) # clamp between 6 and 24 hours
    all_core_walltime = args.all_core_walltime if args.all_core_walltime is not None else all_core_walltime
    executors.append(IPyParallelExecutor(label='all_core',
                     provider=SlurmProvider('pbatch',
                     launcher=SrunLauncher(),
                     nodes_per_block=all_core_nodes,
                     tasks_per_node=1,
                     init_blocks=1,
                     max_blocks=1,
                     walltime=all_core_walltime,
                     overrides=overrides + "\nmodule load cuda/8.0;")))
if two_core_nodes > 0:
    two_core_walltime = get_time_string(clamp((num_jobs * s3_job_time) / two_core_nodes, 7200, 86399)) # clamp between 2 and 24 hours
    two_core_walltime = args.two_core_walltime if args.two_core_walltime is not None else two_core_walltime
    executors.append(IPyParallelExecutor(label='two_core',
                     provider=SlurmProvider('pbatch',
                     launcher=SrunLauncher(),
                     nodes_per_block=two_core_nodes,
                     tasks_per_node=int(cores_per_node / 2),
                     init_blocks=1,
                     max_blocks=1,
                     walltime=two_core_walltime,
                     overrides=overrides)))

print("Running {} subjects. Using {} all-core nodes, {} two-core nodes, and {} one-core nodes.".format(
    num_jobs, all_core_nodes, two_core_nodes, one_core_nodes))

subscripts.config.one_core_executor_labels = ['one_core_head'] + ['one_core'] * one_core_nodes

config = Config(executors=executors)
config.retries = 5
config.checkpoint_mode = 'task_exit'
if not args.force:
    config.checkpoint_files = get_all_checkpoints()
parsl.set_stream_logger()
parsl.load(config)

step_functions = {
    's1':run_s1,
    's2a':run_s2a,
    's2b':run_s2b,
    's3':run_s3,
    's4':run_s4,
}
prereqs = {
    's1':[],
    's2a':['s1'],
    's2b':['s1'],
    's3':['s2a','s2b'],
    's4':['s3'],
}

cores_per_task = {
    's1':1,
    's2a':cores_per_node,
    's2b':cores_per_node,
    's3':2,
    's4':1,
}

# Verify that prerequisite steps are set. Ignore prerequisites for single step runs.
if len(steps) > 1:
    for step in steps:
        for prereq in prereqs[step]:
            if prereq not in steps:
                print("Warning: step {} has prerequisite steps {}".format(step, prereqs[step]))

container = abspath(args.container) if args.container else None
odir = abspath(args.output_dir)
global_timing_log = join(odir, 'global_log', 'timing.csv')
smart_mkdir(odir)
smart_mkdir(join(odir, 'global_log'))
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,cores_per_task,use_gpu")
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
        # if len(steps) > 1:
        #     inputs = [subject_jobs[prereq] for prereq in prereqs[step]]
        #     write(stdout, "Running step {} with inputs {}".format(step, prereqs[step]))
        # else:
        #     write(stdout, "Running step {} alone".format(step))
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
