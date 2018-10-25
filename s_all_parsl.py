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
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,islink,isdir
from os import system,mkdir,remove,environ,makedirs
from math import floor

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list', help='Text file list of subject directories.')
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject')
parser.add_argument('step_choice', choices=['s1','s2a','s2b','s2b_gpu','s3'], type=str.lower, help='Script to run across subjects')
parser.add_argument('--force', help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--probtrackx_edge_list', help='Edges processed by probtrackx, in s3_ediPreproc', default=join("lists","listEdgesEDIAll.txt"))
parser.add_argument('--edi_edge_list', help='Edges used for EDI map, in s3_ediPreproc', default=join("lists","listEdgesEDI.txt"))
parser.add_argument('--log_dir', help='Directory containing subject-specific logs, relative to output dir', default=join("parsl_logs"))
parser.add_argument('--tasks_per_node', help='Override default setting, number of tasks per node', type=int)
parser.add_argument('--nodes_per_block', help='Override default setting, number of nodes per block', type=int)
parser.add_argument('--max_blocks', help='Override default setting, max blocks to request', type=int)
parser.add_argument('--walltime', help='Override default setting, max walltime')
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--container', help='Path to Singularity container image')
parser.add_argument('--local_only', help='Run only on local machine',action='store_true')
args = parser.parse_args()

s1 = args.step_choice == 's1'
s2a = args.step_choice == 's2a'
s2b = args.step_choice == 's2b'
s2b_gpu = args.step_choice == 's2b_gpu'
s3 = args.step_choice == 's3'

# Use only two Slurm executors for now, to prevent requesting unnecessary resources
# 1. Local uses the initially running node's cpus, so we don't waste them.
# 2. Batch requests additional nodes on the same cluster.
# Parsl will distribute tasks between both executors.

def get_executors(_tasks_per_node, _nodes_per_block, _max_blocks, _walltime, _overrides):
    executors = [IPyParallelExecutor(label='local',
                provider=LocalProvider(
                init_blocks=_tasks_per_node,
                max_blocks=_tasks_per_node))]
    if not args.local_only:
        executors.append(IPyParallelExecutor(label='batch',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=_nodes_per_block,
                tasks_per_node=_tasks_per_node,
                init_blocks=1,
                max_blocks=_max_blocks,
                walltime=_walltime,
                overrides=_overrides)))
    return executors

def get_executor_labels(_nodes_per_block, _max_blocks):
    labels = ['local']
    if not args.local_only:
        for i in range(_nodes_per_block * _max_blocks):
            labels.append('batch')
    return labels

def get_walltime(_job_time, _num_jobs, _tasks_per_node, _nodes_per_block, _max_blocks):
    job_secs = get_time_seconds(_job_time)
    total_secs = (_num_jobs * job_secs) / (_tasks_per_node * _nodes_per_block * _max_blocks)
    total_secs = max(job_secs, min(total_secs, 86399)) # clamp between 1 and 24 hours
    return get_time_string(total_secs)

if __name__ == "__main__":
    container = abspath(args.container) if args.container else None
    odir = abspath(args.output_dir)
    smart_mkdir(odir)
    log_dir = join(odir, args.log_dir)
    smart_mkdir(log_dir)

    # Setup defaults for step choice
    num_cores = int(floor(multiprocessing.cpu_count() / 2))
    tasks_per_node = nodes_per_block = max_blocks =  None
    job_time = None # average time per subject per node
    dependencies = []
    slurm_override = "#SBATCH -A {}".format(args.bank)
    if s1:
        tasks_per_node = num_cores
        nodes_per_block = 4
        max_blocks = 1
        job_time = "00:05:00"
    elif s2a:
        tasks_per_node = 1
        nodes_per_block = 3
        max_blocks = 3
        job_time = "00:45:00"
        slurm_override += "\nmodule load cuda/8.0;"
        dependencies = ['s1']
    elif s2b:
        tasks_per_node = 6
        nodes_per_block = 8
        max_blocks = 2
        job_time = "08:00:00"
        if islink(join(odir,"fsaverage")):
            run("unlink {}".format(join(odir,"fsaverage")))
        dependencies = ['s1']
    elif s2b_gpu:
        tasks_per_node = 1
        nodes_per_block = 4
        max_blocks = 2
        job_time = "03:00:00"
        dependencies = ['s1']
    elif s3:
        tasks_per_node = 12 # keep low to avoid memory shortage
        nodes_per_block = 10
        max_blocks = 2
        job_time = "08:00:00" # for default list of 900 edges
        dependencies = ['s2a','s2b']

    # Validate subject inputs
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
        sdir = join(odir, sname)
        if read_checkpoint(sdir, args.step_choice, checksum) and not args.force:
            print("Skipping subject {}. Already ran with step {}. Use --force to re-compute.".format(sname, args.step_choice))
            continue
        if not read_checkpoints(sdir, dependencies, checksum):
            print("Skipping subject {}. Step {} is missing dependencies - must have completed steps {}".format(sname, args.step_choice, dependencies))
            continue

        subjects.append({'input_dir':input_dir, 'sname':sname, 'checksum':checksum})
        print("Running subject {} with step {}".format(sname, args.step_choice))
    num_jobs = len(subjects)
    walltime = get_walltime(job_time, num_jobs, tasks_per_node, nodes_per_block, max_blocks)

    # Override from arguments
    tasks_per_node = args.tasks_per_node if args.tasks_per_node is not None else tasks_per_node
    nodes_per_block = args.nodes_per_block if args.nodes_per_block is not None else nodes_per_block
    max_blocks = args.max_blocks if args.max_blocks is not None else max_blocks
    walltime = args.walltime if args.walltime is not None else walltime
    cores_per_task = max(int(num_cores / tasks_per_node), 1)
    print("Running {} jobs, {} at a time. Max walltime is {}".format(
        num_jobs, tasks_per_node * nodes_per_block * max_blocks, walltime))

    subscripts.config.executor_labels = get_executor_labels(nodes_per_block, max_blocks)
    executors = get_executors(tasks_per_node, nodes_per_block, max_blocks, walltime, slurm_override)
    config = Config(executors=executors)
    config.retries = 2
    parsl.set_stream_logger()
    parsl.load(config) 

    jobs = []
    for subject in subjects:
        input_dir = subject['input_dir']
        sname = subject['sname']
        checksum = subject['checksum']
        sdir = join(odir, sname)
        stdout = join(log_dir, sname + ".stdout")
        smart_mkdir(sdir)
        if s1:
            from subscripts.s1_dti_preproc import create_job
            jobs.append((sname, create_job(input_dir, sdir, stdout, container, checksum)))
        elif s2a:
            from subscripts.s2a_bedpostx import create_job
            jobs.append((sname, create_job(sdir, stdout, container, checksum)))
        elif s2b or s2b_gpu:
            from subscripts.s2b_freesurfer import create_job
            jobs.append((sname, create_job(sdir, s2b_gpu, cores_per_task, stdout, container, checksum, args.force)))
        elif s3:
            from subscripts.s3_probtrackx_edi import create_job
            jobs.append((sname, create_job(sdir, stdout, args.probtrackx_edge_list, args.edi_edge_list, container, checksum)))
    for job in jobs:
        if job[1] is None:
            continue
        sname = job[0]
        try:
            job[1].result()
            print("Finished processing subject {} with step {}".format(sname, args.step_choice))
        except:
            stdout = join(log_dir, sname + ".stdout")
            print("Error: failed to process subject {}. See details in {}".format(sname, stdout))
