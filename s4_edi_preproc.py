#!/usr/bin/env python3
import argparse
import time
import sys    
import os
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
args = parser.parse_args()

start_time = time.time()
print("Running {}".format(os.path.basename(sys.argv[0])))
  
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

run(pbs_cmd)

if args.output_time:
    print("{} took {}".format(os.path.basename(sys.argv[0]), getTimeString(time.time() - start_time)))


