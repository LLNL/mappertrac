#!/usr/bin/env python3
import argparse
import sys
import os
import re
import time
from os.path import exists,join,splitext,abspath
from os import system,mkdir
from glob import glob
from utilities import *
from shutil import rmtree

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
  
odir = abspath(args.output_dir)
if not exists(join(odir, args.pbtk_dir)):
    mkdir(join(odir, args.pbtk_dir))
if exists(join(odir, "qsub")):
    rmtree(join(odir, "qsub"), ignore_errors=True)

if not exists(join(odir, "arguments.list")) or args.force:
    # Assemble all files 
    files = glob(join(odir, args.vol_dir,"*_s2fa.nii.gz"))
    files = [abspath(f) for f in files]

    # Make a temporary argument list
    pairs = open(join(odir, "arguments.list"),"w")

    for f1 in files:
        for f2 in files:
            if f1 != f2:
                pairs.write(f1 + ":" + f2 + "\n")

    pairs.close()

job_runtime = 20 # runtime for each job, in minutes
# if args.force or not exists(join(odir, args.pbtk_dir)):

pbs_cmd = ("python batchMaster.py"
           + " {} {} 1 {}".format(join(odir, "arguments.list"), args.num_jobs, job_runtime)
           + " python s3_subproc.py --input {}".format(odir)
           + " --bedpost_dir {} --pbtk_dir {}".format(args.bedpost_dir, args.pbtk_dir)
           + (" --force" if args.force else ""))
# print(pbs_cmd)
# exit(0)

batch_script = run(pbs_cmd)
print("Requesting batch job: {}".format(batch_script))
job_id = run("msub {}".format(batch_script)).strip()
if not isInteger(job_id):
    print("Failed to queue job {}".format(batch_script))
    exit(0)
print("Successfully requested job {}".format(job_id))

quit_counter = 0
# Wait until job has finished
for i in range(480):
    time.sleep(60) # wait one minute before checking each loop

    job_state = run("sacct -j {} -o State --noheader".format(job_id.strip()), print_output=False).strip()
    print(job_state)
    if job_state == "COMPLETED":
        break
    elif job_state == "FAILED":
        print("Job failed on cluster")
        exit(0)
    elif job_state in ["PENDING", "RUNNING", "COMPLETING"]:
        quit_counter = 0
    elif job_state == "":
        quit_counter += 1
        if quit_counter > 4:
            print("Could not find job on cluster")
            exit(0)

else:
    print("Job timed out on cluster")
    exit(0)



if args.output_time:
    printFinish(start_time)
