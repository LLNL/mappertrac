#!/usr/bin/env python3
import argparse
import sys    
import os
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,rmdir
from shutil import *
from subscripts.utilities import *

parser = argparse.ArgumentParser(description='Run BedpostX')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
parser.add_argument('--use_gpu', help='Use GPU-enabled binaries', action='store_false')
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

if args.use_gpu:
    if 'CUDA_8_LIB_DIR' in environ:
        environ['CUDA_LIB_DIR'] = environ['CUDA_8_LIB_DIR']
    if not 'CUDA_LIB_DIR' in environ:
        print("Environment variable CUDA_LIB_DIR not set.")
        exit(0)

if args.force or not exists(join(bedpostxResults,"dyads3.nii.gz")):
    
    if exists(bedpostxResults):
        rmtree(bedpostxResults)
    
    if args.use_gpu and exists(join(FSLDIR,"bedpostx_gpu")):
        print("Running Bedpostx with GPU")
        run("bedpostx_gpu " + bedpostx + " -NJOBS 4")
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
        rmtree(join(EDI, "bedpostx_b1000.bedpostX"))
    if exists(bedpostxResults):
        copytree(bedpostxResults, join(EDI, "bedpostx_b1000.bedpostX"))
    else:
        print("Failed to generate bedpostX results")
  
if args.output_time:
    printFinish(start_time)