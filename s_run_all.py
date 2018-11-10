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
from math import floor
from subscripts.s1_dti_preproc import run_s1
from subscripts.s2a_bedpostx import run_s2a
from subscripts.s2b_freesurfer import run_s2b
from subscripts.s3_probtrackx import run_s3
from subscripts.s4_edi import run_s4

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list', help='Text file list of subject directories.')
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject')
parser.add_argument('--steps', type=str.lower, help='Steps to run across subjects', default="s1 s2a s2b s3 s4")
parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', default="s2a")
parser.add_argument('--force', help='Force re-compute if checkpoints already exist',action='store_true')
parser.add_argument('--edge_list', help='Edges processed by s3_probtrackx and s4_edi', default=join("lists","listEdgesEDI.txt"))
parser.add_argument('--s2_job_time', help='Average time to finish s2a and s2b on a single subject with a single node', default="08:00:00")
parser.add_argument('--s3_job_time', help='Average time to finish s3 on a single subject with a single node', default="06:00:00")
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--group', help='Unix group to assign file permissions', default='tbidata')
parser.add_argument('--local_only', help='Run only on local machine', action='store_true')
parser.add_argument('--container', help='Path to Singularity container image')
parser.add_argument('--walltime', help='Override default setting, max walltime')
parser.add_argument('--all_core_nodes', help='Override default setting. Number of nodes using all cores on each task. For steps s2a_bedpostx and s2b_freesurfer.')
parser.add_argument('--two_core_nodes', help='Override default setting. Number of nodes with two cores per task. For step s3_probtrackx.')
parser.add_argument('--one_core_nodes', help='Override default setting. Number of nodes with one core per task.')
args = parser.parse_args()

steps = [x.strip() for x in args.steps.split(' ') if x]
gpu_steps = [x.strip() for x in args.gpu_steps.split(' ') if x]

container = abspath(args.container) if args.container else None
odir = abspath(args.output_dir)
global_timing_log = join(odir, 'global_log', 'timing.csv')
smart_mkdir(odir)
smart_mkdir(join(odir, 'global_log'))
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,cores_per_task,use_gpu")

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

all_core_nodes = clamp(num_jobs, 1, 8)
two_core_nodes = clamp(num_jobs, 2, 8)
one_core_nodes = clamp(num_jobs / 2, 2, 8)
# Override from arguments
all_core_nodes = args.all_core_nodes if args.all_core_nodes is not None else all_core_nodes
two_core_nodes = args.two_core_nodes if args.two_core_nodes is not None else two_core_nodes
one_core_nodes = args.one_core_nodes if args.one_core_nodes is not None else one_core_nodes

s2_job_time = get_time_seconds(args.s2_job_time)
s3_job_time = get_time_seconds(args.s3_job_time)
job_time = (num_jobs * s2_job_time) / all_core_nodes + (num_jobs * s3_job_time) / two_core_nodes
walltime = get_time_string(clamp(job_time, 28800, 86399)) # clamp between 8 and 24 hours
walltime = args.walltime if args.walltime is not None else walltime

# We assume 2 vCPUs per core
cores_per_node = int(floor(multiprocessing.cpu_count() / 2))

overrides = "#SBATCH -A {}".format(args.bank)

executors = [
IPyParallelExecutor(label='one_core_head',
                provider=LocalProvider(
                init_blocks=cores_per_node,
                max_blocks=cores_per_node)),
IPyParallelExecutor(label='all_core',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=all_core_nodes,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                walltime=walltime,
                overrides=overrides + "\nmodule load cuda/8.0;")),
IPyParallelExecutor(label='two_core',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=two_core_nodes,
                tasks_per_node=int(cores_per_node / 2),
                init_blocks=1,
                max_blocks=1,
                walltime=walltime,
                overrides=overrides)),
IPyParallelExecutor(label='one_core',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=one_core_nodes,
                tasks_per_node=cores_per_node,
                init_blocks=1,
                max_blocks=1,
                walltime=walltime,
                overrides=overrides)),
]

print("Running {} subjects. Using {} all-core nodes, {} two-core nodes, and {} one-core nodes. Max walltime is {}".format(
    num_jobs, all_core_nodes, two_core_nodes, one_core_nodes, walltime))

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
                raise Exception("Step {} requires steps {} as well".format(step, prereqs[step]))

all_jobs = []
for subject in subjects:
    input_dir = subject['input_dir']
    sname = subject['sname']
    checksum = subject['checksum']
    sdir = join(odir, sname)
    log_dir = join(sdir,'log')
    params = {
        'input_dir': input_dir,
        'sdir': sdir,
        'container': container,
        'checksum': checksum,
        'edge_list': args.edge_list,
        'group': args.group,
        'global_timing_log': global_timing_log,
    }
    smart_mkdir(log_dir)
    smart_mkdir(sdir)

    subject_jobs = {}
    inputs = []
    
    for step in steps:
        stdout, idx = get_valid_filepath(join(log_dir, step + ".stdout"))
        timing_log = join(log_dir, "{}_{:02d}_timing.txt".format(step, idx))
        smart_remove(timing_log)
        params['cores_per_task'] = cores_per_task[step]
        params['use_gpu'] = step in gpu_steps
        params['stdout'] = stdout
        params['timing_log'] = timing_log
        params['step'] = step
        # Use jobs from previous steps as inputs for current step
        if len(steps) > 1:
            inputs = [subject_jobs[prereq] for prereq in prereqs[step]]
        job = step_functions[step](params, inputs)
        subject_jobs[step] = job
        all_jobs.append((sname, job))
        
for job in all_jobs:
    if job[1] is None:
        continue
    sname = job[0]
    try:
        job[1].result()
        print("Finished processing subject {}".format(sname))
    except:
        print("Error: failed to process subject {}".format(sname))
