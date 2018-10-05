#!/usr/bin/env python3
import argparse
import sys
import os
import multiprocessing
import parsl
from mpi4py import MPI
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.threads import ThreadPoolExecutor
from libsubmit.providers import LocalProvider
from libsubmit.channels import LocalChannel
from glob import glob
from shutil import *
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,makedirs
from parsl.configs.local_threads import config

comm = MPI.COMM_WORLD

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

config = Config(
    executors=[
        IPyParallelExecutor(
            label="single_node",
            provider=LocalProvider(
                init_blocks=multiprocessing.cpu_count(),
                max_blocks=multiprocessing.cpu_count(),
            )
        )
        # ThreadPoolExecutor(
        #     max_threads=multiprocessing.cpu_count(),
        # )
    ],
    # run_dir="runinfo{}".format(comm.rank)
)
parsl.set_stream_logger()
parsl.load(config)

@python_app(executors=["single_node"])
def eddy_correct(section, output_prefix):
    from subscripts.utilities import run
    # import subprocess
    # from os import system,environ
    # from subprocess import Popen,PIPE
    # def run(command):
    #     process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ)
    #     line = ""
    #     while True:
    #         new_line = str(process.stdout.readline(),'utf-8')[:-1]
    #         if new_line:
    #             print(new_line)
    #         if new_line == '' and process.poll() is not None:
    #             break
    #         line = new_line
    #     if process.returncode != 0 and not ignore_errors:
    #         raise Exception("Non zero return code: {}".format(process.returncode))
    #     return line
    run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1 >> {1}.ecclog".format(section, output_prefix))

idir = abspath(args.input_dir)
if not exists(abspath(args.output_dir)):  # make sure the output dir exists
    makedirs(abspath(args.output_dir))

odir = abspath(args.output_dir)

# Make sure that we use a patient directory as output
if split(idir)[-1] != split(odir)[-1]:
    # if not we create the patient directory
    odir = join(odir,split(idir)[-1])
    if not exists(odir):
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
rd = dti_params + "_RD.nii.gz"
md = dti_params + "_MD.nii.gz"

# If necessary copy the data into the target directory
smart_copy(join(idir,"hardi.nii.gz"),data,args.force)
smart_copy(join(idir,"bvecs"),bvecs)
smart_copy(join(idir,"bvals"),bvals)
smart_copy(join(idir,"anat.nii.gz"),T1)

# Start the eddy correction (3 minutes)
if args.force or not exist_all([eddy,bet,bvecs_rotated,fa,rd,md]):
    if exists(eddy_log):
        remove(eddy_log)
    print("Eddy correction")

    input_prefix = join(odir,"data")
    output_prefix = join(odir,"data_eddy")

    run("fslroi {} {}_ref 0 1".format(input_prefix,output_prefix))
    run("fslsplit {} {}_tmp".format(input_prefix,output_prefix))
    full_list = glob(join("{}_tmp????.*").format(output_prefix))

    jobs = []
    for section in full_list:
        print(section)
        jobs.append(eddy_correct(section, output_prefix))
    for job in jobs:
        job.result()
    run("fslmerge -t {} {}".format(eddy, " ".join(full_list)))
    for i in full_list:
        remove(i)
    for j in glob(join("{}_ref*").format(output_prefix)):
        remove(j)

    print("Brain extraction")
    run("bet {} {} -m -f 0.3".format(eddy,bet),output_time=True)

    print("Rotating")
    run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log),output_time=True)

    run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(eddy,dti_params,bet_mask,bvecs,bvals),output_time=True)
    run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_params + "_L1.nii.gz",dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",md),output_time=True)
    run("fslmaths {} -add {}  -div 2 {} ".format(dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",rd),output_time=True)

    copy(dti_params + "_L1.nii.gz",dti_params + "_AD.nii.gz")
    copy(dti_params + "_FA.nii.gz",fa)

if args.output_time:
    printFinish(start_time)