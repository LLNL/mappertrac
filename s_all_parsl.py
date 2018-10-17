#!/usr/bin/env python3
import argparse
import multiprocessing
import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers import LocalProvider,SlurmProvider
from libsubmit.launchers import SrunLauncher
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,islink
from os import system,mkdir,remove,environ,makedirs
from math import floor

parser = argparse.ArgumentParser(description='Generate connectome data', usage="%(prog)s subject_list output_dir [--force] [--edge_list EDGE_LIST] [s1] [s2a] [s2b] [s3] [s4]\n")
parser.add_argument('input_dirs', help='Pathname(s) to input data, parsed with glob')
parser.add_argument('output_dir', help='The super-directory that will contain output directories for each subject')
parser.add_argument('step_choice', choices=['s1','s2a','s2b','s2b_gpu','s3'], type=str.lower, help='Script to run across subjects')
parser.add_argument('--force', help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--edge_list', help='Edges processed by probtrackx, in s3_ediPreproc', default=join("lists","listEdgesEDI.txt"))
parser.add_argument('--log_dir', help='Directory containing log output', default=join("parsl_logs"))
parser.add_argument('--tasks_per_node', help='Override default setting, number of tasks per node', type=int)
parser.add_argument('--nodes_per_block', help='Override default setting, number of nodes per block', type=int)
parser.add_argument('--max_blocks', help='Override default setting, max blocks to request', type=int)
parser.add_argument('--walltime', help='Override default setting, max walltime')
parser.add_argument('--bank', help='Slurm bank to charge for jobs', default="ccp")
parser.add_argument('--subject_list', help='Text file with list of subject directories.')
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
    return [IPyParallelExecutor(label='local',
                provider=LocalProvider(
                init_blocks=_tasks_per_node,
                max_blocks=_tasks_per_node)),
            IPyParallelExecutor(label='batch',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=_nodes_per_block,
                tasks_per_node=_tasks_per_node,
                init_blocks=1,
                max_blocks=_max_blocks,
                walltime=_walltime,
                overrides=_overrides))
            ]

def get_executor_labels(_nodes_per_block, _max_blocks):
    labels = ['local']
    for i in range(_nodes_per_block * _max_blocks):
        labels.append('batch')
    return labels

num_cores = int(floor(multiprocessing.cpu_count() / 2))
tasks_per_node = nodes_per_block = max_blocks = walltime = None
slurm_override = "#SBATCH -A {}".format(args.bank)

if s1:
    tasks_per_node = num_cores
    nodes_per_block = 4
    max_blocks = 1
    walltime = "03:00:00"
elif s2a:
    tasks_per_node = 1
    nodes_per_block = 4
    max_blocks = 2
    walltime = "12:00:00"
    slurm_override += "\nmodule load cuda/8.0;"
elif s2b:
    tasks_per_node = 1
    nodes_per_block = 8
    max_blocks = 2
    walltime = "23:59:00"
elif s2b_gpu:
    tasks_per_node = 1
    nodes_per_block = 4
    max_blocks = 2
    walltime = "12:00:00"
elif s3:
    tasks_per_node = num_cores
    nodes_per_block = 4
    max_blocks = 1
    walltime = "12:00:00"
tasks_per_node = args.tasks_per_node if args.tasks_per_node is not None else tasks_per_node
nodes_per_block = args.nodes_per_block if args.nodes_per_block is not None else nodes_per_block
max_blocks = args.max_blocks if args.max_blocks is not None else max_blocks
walltime = args.walltime if args.walltime is not None else walltime
cores_per_task = int(num_cores / tasks_per_node)
executors = get_executors(tasks_per_node, nodes_per_block, max_blocks, walltime, slurm_override)
executor_labels = get_executor_labels(nodes_per_block, max_blocks)

if __name__ == "__main__":
    config = Config(executors=executors)
    config.retries = 2
    parsl.set_stream_logger()
    parsl.load(config) 

    odir = abspath(args.output_dir)
    smart_mkdir(odir)
    jobs = []
    edges = []
    log_dir = join(odir, args.log_dir)
    smart_mkdir(log_dir)
    if s2b and islink(join(odir,"fsaverage")):
        run("unlink {}".format(join(odir,"fsaverage")))
    input_dirs = glob(args.input_dirs)
    for input_dir in input_dirs:
        input_dir = input_dir.strip()
        checksum = generate_checksum(input_dir)
        subject = basename(input_dir)
        sdir = join(odir, subject)
        stdout = join(log_dir, basename(sdir) + ".stdout")
        smart_mkdir(sdir)
        if read_checkpoint(sdir, args.step_choice, checksum) and not args.force:
            write_output(stdout, "Already ran subject through step {}. Use --force to re-compute.".format(args.step_choice))
            continue
        if s1:
            from subscripts.s1_dti_preproc import create_job
            jobs.append((subject, create_job(input_dir, sdir, stdout, checksum)))
        elif s2a:
            from subscripts.s2a_bedpostx import create_job
            jobs.append((subject, create_job(sdir, stdout, checksum)))
        elif s2b or s2b_gpu:
            from subscripts.s2b_freesurfer import create_job
            jobs.append((subject, create_job(sdir, stdout, checksum)))
        elif s3:
            from subscripts.s3_probtrackx_edi import create_job
            jobs.append((subject, create_job(sdir, stdout, checksum)))
    for job in jobs:
        if job is None:
            continue
        try:
            job[1].result()
        except:
            print("Error: failed to process subject {}".format(job[0]))
