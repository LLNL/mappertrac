#!/usr/bin/env python3
import argparse
import sys
import os
import re
import time
from os.path import exists,join,splitext,abspath
from os import system
from glob import glob
from utilities import *

parser = argparse.ArgumentParser(description='Preprocess EDI data')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--vol_dir', help='The directory containing Freesurfer region output, relative to output directory', default="EDI/allvols")
parser.add_argument('--num_jobs', help='Number of parallel jobs (i.e. threads)', default=6643)
parser.add_argument('--bedpost_dir', help='Path to generate bedpost output, relative to output directory', default='bedpostx_b1000.bedpostX')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--time_limit', help='Maximum time to wait for job completion, in minutes', default=480)
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

start_time = printStart()
  
odir = abspath(args.output_dir)
bdir = join(odir, args.bedpost_dir)
vdir = join(odir, args.vol_dir)
# Assemble all files 
files = glob(join(odir, args.vol_dir,"*_s2fa.nii.gz"))
files = [abspath(f) for f in files]

# Make a temporary argument list
pairs = open("pairs.list","w")

for f1 in files:
    for f2 in files:
        if f1 != f2:
            pairs.write(f1 + ":" + f2 + "\n")

pairs.close()

pbs_cmd = ("python batchMaster.py"
           + " pairs.list {} 1 10".format(args.num_jobs)
           + " python s4_subproc.py --input {}".format(odir))

batch_script = run(pbs_cmd)
job_id = run("msub {}".format(batch_script)).strip()
if not isInteger(job_id):
    print("Failed to queue job {}".format(batch_script))
    exit(0)

# Wait until job has finished
for i in range(480):
    job_state = run("sacct -j {} -o State --noheader".format(job_id.strip()), print_output=False).strip()
    if job_state == "COMPLETED":
        break
    elif job_state == "FAILED":
        print("Job failed on cluster")
        exit(0)
    elif job_state == "":
        print("Could not find job on cluster")
        exit(0)

    time.sleep(60) # wait one minute before checking again
else:
    print("Job timed out on cluster")
    exit(0)

if args.output_time:
    printFinish(start_time)

