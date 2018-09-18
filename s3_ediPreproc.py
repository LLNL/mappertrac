#!/usr/bin/env python3
import argparse
import sys
import os
import re
import time
import parsl
import multiprocessing
from os.path import exists,join,splitext,abspath
from os import system,mkdir,environ
from glob import glob
from subscripts.utilities import *
from shutil import rmtree
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers import SlurmProvider, LocalProvider
from libsubmit.channels import LocalChannel

parser = argparse.ArgumentParser(description='Preprocess EDI data')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--vol_dir', help='The directory containing Freesurfer region output, relative to output directory', default="EDI/allvols")
parser.add_argument('--num_jobs', help='Number of parallel jobs (i.e. threads)', default=6643)
parser.add_argument('--bedpost_dir', help='The directory containing bedpost output, relative to output directory', default='bedpostx_b1000.bedpostX')
parser.add_argument('--pbtk_dir', help='The directory to place region pair output, relative to output directory',
                    default=join("EDI","PBTKresults"))
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--time_limit', help='Maximum time to wait for job completion, in minutes', default=480)
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

start_time = printStart()

config = Config(
    executors=[

        IPyParallelExecutor(
            label='test_singlenode',
            provider=LocalProvider(
                init_blocks=multiprocessing.cpu_count(),
                max_blocks=multiprocessing.cpu_count(),
            )
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
        )
    ],
    retries=3,
    checkpoint_mode = 'dfk_exit'
)
parsl.load(config)

@python_app
def subproc(src_and_target, output_dir, force=False):
    from subscripts.utilities import run
    run("python3 s3_subproc.py {} {} {}".format(src_and_target, output_dir, "--force" if force else ""), write_output="s3_subproc.stdout")
  
odir = abspath(args.output_dir)
if not exists(join(odir, args.pbtk_dir)):
    mkdir(join(odir, args.pbtk_dir))
if exists(join(odir, "qsub")):
    rmtree(join(odir, "qsub"), ignore_errors=True)
if not 'FSLDIR' in environ:
    print("FSLDIR not found in local environment")
    exit(0)

jobs = []
files = glob(join(odir, args.vol_dir,"*_s2fa.nii.gz")) # Assemble all files 
files = [abspath(f) for f in files]

for f1 in files:
    for f2 in files:
        if f1 != f2:
            if len(jobs) > 50:
                break
            jobs.append(subproc("{}:{}".format(f1, f2), odir, args.force))
            print("Starting {} to {}".format(f1, f2))
    if len(jobs) > 50:
        break
for job in jobs:
    job.result()

if args.output_time:
    printFinish(start_time)

