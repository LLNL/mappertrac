#!/usr/bin/env python3
import argparse
import sys    
import os
import shutil
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,rmdir
from subscripts.utilities import *

parser = argparse.ArgumentParser(description='Run BedpostX')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
parser.add_argument('--disable_gpu', help='Do not use GPU-enabled binaries', action='store_true')
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
bedpostxResults = join(odir,"bedpostx_b1000.bedpostX")
EDI = join(odir, "EDI")

smart_copy(join(odir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),args.force)
smart_copy(join(odir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),args.force)
smart_copy(join(odir,"bvals"),join(bedpostx,"bvals"),args.force)
smart_copy(join(odir,"bvecs"),join(bedpostx,"bvecs"),args.force)

if not args.disable_gpu:
    if 'CUDA_8_LIB_DIR' in environ:
        environ['CUDA_LIB_DIR'] = environ['CUDA_8_LIB_DIR']
        environ['LD_LIBRARY_PATH'] = "{}:{}".format(environ['CUDA_LIB_DIR'],environ['LD_LIBRARY_PATH'])
        print("Loaded CUDA 8.0 library")
    else:
        print("Environment variable CUDA_8_LIB_DIR not set.")
        exit(0)

if args.force or not exists(join(bedpostxResults,"dyads3.nii.gz")):
    # if exists(bedpostxResults):
    #     print("Removing old bedpostx results")
    #     shutil.rmtree(bedpostxResults)

    if not args.disable_gpu:
        if exists(join(FSLDIR,"bedpostx_gpu")):
            print("Running Bedpostx with GPU")
            run("bedpostx_gpu " + bedpostx + " -NJOBS 4")
            exit(0)
        else:
            print("Failed to find {}".format(join(FSLDIR,"bedpostx_gpu")))
            exit(0)
    else:
        print("Running Bedpostx without GPU")
        run("bedpostx " + bedpostx)

if args.force or not exists(join(bedpostxResults, "dyads2_dispersion.nii.gz")):
    bed_dir = join(odir, "bedpostx_b1000.bedpostX")
    run("make_dyadic_vectors {} {} {} {}".format(join(bedpostxResults, "merged_th1samples"),
                                                 join(bedpostxResults, "merged_ph1samples"),
                                                 join(bedpostxResults, "nodif_brain_mask"),
                                                 join(bedpostxResults, "dyads1")))

    run("make_dyadic_vectors {} {} {} {}".format(join(bedpostxResults, "merged_th2samples"),
                                                 join(bedpostxResults, "merged_ph2samples"),
                                                 join(bedpostxResults, "nodif_brain_mask"),
                                                 join(bedpostxResults, "dyads2")))

if args.force or not exists(join(EDI, "bedpostx_b1000.bedpostX")):
    if exists(join(EDI, "bedpostx_b1000.bedpostX")):
        shutil.rmtree(join(EDI, "bedpostx_b1000.bedpostX"))
    if exists(bedpostxResults):
        shutil.copytree(bedpostxResults, join(EDI, "bedpostx_b1000.bedpostX"))
    else:
        print("Failed to generate bedpostX results")
  
if args.output_time:
    printFinish(start_time)