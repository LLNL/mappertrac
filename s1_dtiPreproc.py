#!/usr/bin/env python3
import argparse
import sys
import os
from shutil import *
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,makedirs

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('input_dir',help='The directory with the input dataset '
                    'formatted according to the BIDS standard.')
parser.add_argument('output_dir',help='The directory where the output files '
                    'should be stored. The last directory of the output_dir '
                    'should be the same as the last directory of the input_dir '
                    '(the patient directory. If not the patient directory will '
                    'be created')
parser.add_argument('--force',help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--output_time',help='Print completion time',action='store_true')
args = parser.parse_args()

start_time = printStart()

# load_environ()
idir = abspath(args.input_dir)
if not exists(abspath(args.output_dir)):  # make sure the output dir exists
    makedirs(abspath(args.output_dir))

odir = abspath(args.output_dir)

# Make sure that we use a patient directory as output
if split(idir)[-1] != split(odir)[-1]:
    # if not we create the patient directory
    odir = join(odir,split(idir)[-1])
    makedirs(odir)

# Create a list of files that this script is supposed to produce
data = join(odir,"data.nii.gz")  # The target data file
eddy = join(odir,"data_eddy.nii.gz")
eddy_log = join(odir,"data_eddy.ecclog")
bet = join(odir,"data_bet.nii.gz")
bet_mask = join(odir,"data_bet_mask.nii.gz")
bvecs = join(odir,"bvecs")
bvecs_rotated = join(odir,"bvecs_rotated")
bvals = join(odir,"bvals")
dti_params = join(odir,"DTIparams")
fa = join(odir,"FA.nii.gz")
T1 = join(odir,"T1.nii.gz")

# If necessary copy the data into the target directory
smart_copy(join(idir,"hardi.nii.gz"),data,args.force)
smart_copy(join(idir,"bvecs"),bvecs)
smart_copy(join(idir,"bvals"),bvals)
smart_copy(join(idir,"anat.nii.gz"),T1)

# run("echo 'hello'")

# Start the eddy correction (30 minutes)
if args.force or not exists(eddy):
    if exists(eddy_log):
        remove(eddy_log)
    print("Eddy correction")
    run("eddy_correct {} {} 0".format(data,eddy),output_time=True)

if args.force or not exists(bet):
    print("Brain extraction")
    run("bet {} {} -m -f 0.3".format(eddy,bet),output_time=True)

if args.force or not exists(bvecs_rotated):
    print("Rotating")
    run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log),output_time=True)

if args.force or not exists(dti_params + "_MD.nii.gz"):
    run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(eddy,dti_params,bet_mask,bvecs,bvals),output_time=True)
    run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_params + "_L1.nii.gz",dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_MD.nii.gz"),output_time=True)
    run("fslmaths {} -add {}  -div 2 {} ".format(dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_RD.nii.gz"),output_time=True)

    copy(dti_params + "_L1.nii.gz",dti_params + "_AD.nii.gz")
    copy(dti_params + "_FA.nii.gz",fa)

if args.output_time:
    printFinish(start_time)
