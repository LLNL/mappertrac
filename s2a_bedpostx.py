#!/usr/bin/env python3
import argparse
import sys    
import os
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,rmdir
from shutil import *
from utilities import *

parser = argparse.ArgumentParser(description='Run BedpostX')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

start_time = printStart()

odir = abspath(args.output_dir)

if not exists(join(odir,"bedpostx_b1000")):
    mkdir(join(odir,"bedpostx_b1000"))

# Check whether the right environment variables are set
FSLDIR = join(environ["FSLDIR"],"bin")
if not exists(FSLDIR):
    print("Cannot find FSL_DIR environment variable")
    exit(0)

bedpostx = join(odir,"bedpostx_b1000")

smart_copy(join(odir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),args.force)
smart_copy(join(odir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),args.force)
smart_copy(join(odir,"bvals"),join(bedpostx,"bvals"),args.force)
smart_copy(join(odir,"bvecs"),join(bedpostx,"bvecs"),args.force)

if args.force or not exists(join(odir,"bedpostx_b1000.bedpostX","dyads3.nii.gz")):
    
    if exists(join(odir,"bedpostx_b1000.bedpostX")):
        rmtree(join(odir,"bedpostx_b1000.bedpostX"))
            
    
    if exists(join(FSLDIR,"bedpostx_gpu")):
        run("bedpostx_gpu " + bedpostx + " -NJOBS 4")
    else:
        # print(bedpostx)
        run("bedpostx " + bedpostx)
  
if args.output_time:
    printFinish(start_time)