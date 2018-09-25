#!/usr/bin/env python3
import argparse
import sys
import os
import re
import time
import parsl
import multiprocessing
from os.path import exists,join,splitext,abspath
from os import system,mkdir,environ,remove
from glob import glob
from subscripts.utilities import *
from shutil import rmtree
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.threads import ThreadPoolExecutor
from libsubmit.providers import SlurmProvider, LocalProvider
from libsubmit.channels import LocalChannel

parser = argparse.ArgumentParser(description='Preprocess EDI data')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--vol_dir', help='The directory containing Freesurfer region output, relative to output directory', default=join("EDI","allvols"))
parser.add_argument('--num_jobs', help='Number of parallel jobs (i.e. threads)', default=6643)
parser.add_argument('--bedpost_dir', help='The directory containing bedpost output, relative to output directory', default='bedpostx_b1000.bedpostX')
parser.add_argument('--pbtk_dir', help='The directory to place region pair output, relative to output directory',
                    default=join("EDI","PBTKresults"))
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--time_limit', help='Maximum time to wait for job completion, in minutes', default=480)
parser.add_argument('--output_time', help='Print completion time', action='store_true')
parser.add_argument('--edge_list', help='Text list of edges for consensus EDI', default="lists/listEdgesEDI.txt")
parser.add_argument('--process_all', help='Run probtrackx2 on every region combination, not just those in edge_list', action='store_true')
args = parser.parse_args()

start_time = printStart()

config = Config(
    executors=[
        ThreadPoolExecutor(
            max_threads=multiprocessing.cpu_count(),
            label='local_threads'
        )
#         IPyParallelExecutor(
#             label='test_singlenode',
#             provider=LocalProvider(
#                 init_blocks=multiprocessing.cpu_count(),
#                 max_blocks=multiprocessing.cpu_count(),
#             )
#             provider=SlurmProvider(
#                 'pbatch',
#                 channel=LocalChannel(),
#                 tasks_per_node=18,
#                 nodes_per_block=4,
#                 max_blocks=1,
#                 walltime="01:00:00",
#                 overrides="""
# #SBATCH -A asccasc
# #SBATCH -p pbatch
# #SBATCH -J tbi_s3""",
#             )
#         )
    ],
    retries=3,
    checkpoint_mode = 'dfk_exit'
)
parsl.load(config)

@python_app
def subproc(src, target, odir, bdir, pdir, force, stdout="s3_ediPreproc.stdout", stderr="s3_ediPreproc.stderr"):
    from subscripts.utilities import run
    from os.path import exists,join,splitext,abspath,split
    src_name = splitext(splitext(split(src)[1])[0])[0]
    target_name = splitext(splitext(split(target)[1])[0])[0]
    edge_name = "{}to{}".format(src_name, target_name)
    save_file = "{}to{}.nii.gz".format(src_name,target_name)

    if exists(join(pdir,save_file)) and not force:
        print("Already calculated edge from {} to {}. Use --force to re-compute.".format(src_name, target_name))
        return

    print("Running subproc: {} to {}".format(src_name, target_name))
    waypoints = join(odir,"tmp","tmp_waypoint_{}.txt".format(edge_name))
    exclusion = join(odir,"tmp","tmp_exclusion_{}.nii.gz".format(edge_name))
    termination = join(odir,"tmp","tmp_termination_{}.nii.gz".format(edge_name))

    run("fslmaths {} -sub {} {}".format(join(odir,"EDI","allvoxelscortsubcort.nii.gz"), src, exclusion))
    run("fslmaths {} -sub {} {}".format(exclusion, target, exclusion))
    run("fslmaths {} -add {} {}".format(exclusion, join(odir,"EDI","bs.nii.gz"), exclusion))
    run("fslmaths {} -add {} {}".format(join(odir,"EDI","terminationmask.nii.gz"), target, termination))

    with open((waypoints),"w") as f:
        f.write(target + "\n")

    arguments = (" -x {} ".format(src)
        + " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000"
        + " --waypoints={} --avoid={} --stop={}".format(waypoints, exclusion, termination)
        + " --forcedir --opd"
        + " -s {}".format(join(bdir,"merged"))
        + " -m {}".format(join(bdir,"nodif_brain_mask.nii.gz"))
        + " --dir={}".format(join(pdir))
        + " --out={}".format(save_file)
        + " --omatrix1"
    )
    run("probtrackx2" + arguments)
    print("Finished subproc: {} to {}".format(src_name, target_name))
    # run("python3 s3_subproc.py {} {} {}".format(src_and_target, output_dir, "--force" if force else ""), write_output="s3_subproc.stdout")
  
odir = abspath(args.output_dir)
bdir = join(odir, args.bedpost_dir)
pdir = join(odir, args.pbtk_dir)
if not exists(bdir):
    print("BedpostX directory does not exist at {}".format(bdir))
    exit(0)
if args.force and exists(pdir):
    rmtree(pdir)
if not exists(pdir):
    mkdir(pdir)
if not 'FSLDIR' in environ:
    print("FSLDIR not found in local environment")
    exit(0)

jobs = []
edges = []

if args.process_all:
    files = glob(join(odir, args.vol_dir,"*_s2fa.nii.gz")) # Assemble all files 
    files = [abspath(f) for f in files]
    for a in files:
        for b in files:
            if a != b:
                edges.append([a, b])
else:
    with open(args.edge_list) as f:
        for edge in f.readlines():
            a, b = edge.strip().split(',', 1)
            a = join(odir, args.vol_dir, "{}.nii.gz".format(a))
            if not exists(a):
                print("Could not find {}".format(a))
                continue
            b = join(odir, args.vol_dir, "{}.nii.gz".format(b))
            if not exists(b):
                print("Could not find {}".format(b))
                continue
            edges.append([a, b])

for edge in edges:
    # if len(jobs) > 12:
        # break
    edge_name = "{}:{}".format(edge[0], edge[1])
    jobs.append([edge_name, subproc(edge[0], edge[1], odir, bdir, pdir, args.force)])

for job in jobs:
    try:
        job[1].result()
    except Exception as e:
        print("Failed job {}".format(job[0]))

for i in glob(join(odir,"tmp","tmp_*")):
    remove(i)

if args.output_time:
    printFinish(start_time)

