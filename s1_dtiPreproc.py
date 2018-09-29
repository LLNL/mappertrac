#!/usr/bin/env python3
import argparse
import sys
import os
import multiprocessing
import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers import LocalProvider
from libsubmit.channels import LocalChannel
from glob import glob
from shutil import *
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename
from os import system,mkdir,remove,environ,makedirs

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list',help='Text file with list of subject directories.')
parser.add_argument('output_dir',help='The super-directory that will contain output directories for each subject')
parser.add_argument('--force',help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--output_time',help='Print completion time',action='store_true')
args = parser.parse_args()

start_time = printStart()

config = Config(
    executors=[
        IPyParallelExecutor(
            label='single_node',
            provider=LocalProvider(
                init_blocks=multiprocessing.cpu_count(),
                max_blocks=multiprocessing.cpu_count(),
            )
        )
    ]
)

parsl.load(config)

@python_app
def j1_split_timesteps(input_dir, sdir, outputs=[]):
    from subscripts.utilities import run, smart_copy
    from os.path import join
    smart_copy(join(input_dir,"bvecs"),join(sdir,"bvecs"))
    smart_copy(join(input_dir,"bvals"),join(sdir,"bvals"))
    smart_copy(join(input_dir,"anat.nii.gz"),join(sdir,"T1.nii.gz"))
    input_data = join(input_dir, "hardi.nii.gz")
    run("fslroi {} {}data_eddy_ref 0 1".format(input_data, sdir))
    run("fslsplit {} {}data_eddy_tmp".format(input_data, sdir))

@python_app
def j2_timestep_process(sdir, step, inputs=[], outputs=[]):
    from subscripts.utilities import run
    from os.path import join
    img = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(step))
    if not exists(img):
        print("Failed to open timestep image {}".format(img))
    sdir_prefix = join(sdir,"data_eddy")
    run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1 >> {1}.ecclog".format(img, sdir_prefix))

@python_app
def j3_dti_fit(input_dir, sdir, inputs=[]):
    from subscripts.utilities import run, smart_copy
    from os.path import join
    from glob import glob
    from os import remove
    sdir_prefix = join(sdir,"data_eddy")
    data_eddy = join(sdir,"data_eddy.nii.gz")
    timesteps = glob("{}_tmp????.*".format(sdir_prefix))
    run("fslmerge -t {} {}".format(data_eddy, " ".join(timesteps)))
    for i in timesteps:
        remove(i)
    for j in glob("{}_ref*".format(sdir_prefix)):
        remove(j)

    eddy_log = join(sdir,"data_eddy.ecclog")
    bet = join(sdir,"data_bet.nii.gz")
    bet_mask = join(sdir,"data_bet_mask.nii.gz")
    bvecs = join(sdir,"bvecs")
    # bvecs_rotated = join(sdir,"bvecs_rotated")
    bvals = join(sdir,"bvals")
    dti_params = join(sdir,"DTIparams")
    fa = join(sdir,"FA.nii.gz")
    run("bet {} {} -m -f 0.3".format(data_eddy,bet))
    # run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log))
    run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(data_eddy,dti_params,bet_mask,bvecs,bvals))
    run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_params + "_L1.nii.gz",dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_MD.nii.gz"))
    run("fslmaths {} -add {}  -div 2 {} ".format(dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_RD.nii.gz"))

    smart_copy(dti_params + "_L1.nii.gz",dti_params + "_AD.nii.gz")
    smart_copy(dti_params + "_FA.nii.gz",fa)

odir = abspath(args.output_dir)
jobs = []
with open(args.subject_list) as f:
    for input_dir in f.readlines():
        input_dir = input_dir.strip()
        input_data = join(input_dir, "hardi.nii.gz")
        sdir = join(odir, basename(input_dir))
        num_timesteps = run("fslinfo {} | sed -n -e '/^dim4/p'".format(input_data)).split()[-1]
        print(num_timesteps)
        if not isInteger(num_timesteps):
            print("Failed to read timesteps from {}".format(input_data))
            continue
        j1_future = j1_split_timesteps(input_dir, sdir)
        # jobs.append(j1_future)
        j2_futures = []
        for i in range(int(num_timesteps)):
            j2_future = j2_timestep_process(sdir, i, inputs=[i.outputs[0] for i in output_files], outputs=["all.txt"])
            j2_futures.append(j2_future)
        # jobs.extend(j2_futures)
        inputs = []
        j3_future = j3_dti_fit(input_dir, sdir, j2_futures)
        jobs.append(j3_future)
for job in jobs:
    job.result()
if args.output_time:
    printFinish(start_time)

# idir = abspath(args.input_dir)
# if not exists(abspath(args.output_dir)):  # make sure the output dir exists
#     makedirs(abspath(args.output_dir))

# odir = abspath(args.output_dir)

# # Make sure that we use a patient directory as output
# if split(idir)[-1] != split(odir)[-1]:
#     # if not we create the patient directory
#     odir = join(odir,split(idir)[-1])
#     if not exists(odir):
#         makedirs(odir)

# # Create a list of files that this script is supposed to produce
# data = join(odir,"data.nii.gz")  # The target data file
# eddy = join(odir,"data_eddy.nii.gz")
# eddy_log = join(odir,"data_eddy.ecclog")
# bet = join(odir,"data_bet.nii.gz")
# bet_mask = join(odir,"data_bet_mask.nii.gz")
# bvecs = join(odir,"bvecs")
# bvecs_rotated = join(odir,"bvecs_rotated")
# bvals = join(odir,"bvals")
# dti_params = join(odir,"DTIparams")
# fa = join(odir,"FA.nii.gz")
# T1 = join(odir,"T1.nii.gz")

# # If necessary copy the data into the target directory
# smart_copy(join(idir,"hardi.nii.gz"),data,args.force)
# smart_copy(join(idir,"bvecs"),bvecs)
# smart_copy(join(idir,"bvals"),bvals)
# smart_copy(join(idir,"anat.nii.gz"),T1)

# # Start the eddy correction (3 minutes)
# if args.force or not exists(eddy):
#     if exists(eddy_log):
#         remove(eddy_log)
#     print("Eddy correction")

#     input_prefix = join(odir,"data")
#     output_prefix = join(odir,"data_eddy")

#     run("fslroi {} {}_ref 0 1".format(input_prefix,output_prefix))
#     run("fslsplit {} {}_tmp".format(input_prefix,output_prefix))
#     full_list = glob(join("{}_tmp????.*").format(output_prefix))

#     jobs = []
#     for section in full_list:
#         print(section)
#         jobs.append(eddy_correct(section, output_prefix))
#         # run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1".format(i, eddy))
#     for job in jobs:
#         job.result()
#     run("fslmerge -t {} {}".format(eddy, " ".join(full_list)))
#     for i in full_list:
#         remove(i)
#     for j in glob(join("{}_ref*").format(output_prefix)):
#         remove(j)

#     # run("eddy_correct {} {} 0".format(data,eddy),output_time=True)

# if args.force or not exists(bet):
#     print("Brain extraction")
#     run("bet {} {} -m -f 0.3".format(eddy,bet),output_time=True)

# if args.force or not exists(bvecs_rotated):
#     print("Rotating")
#     run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log),output_time=True)

# if args.force or not exists(dti_params + "_MD.nii.gz"):
#     run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(eddy,dti_params,bet_mask,bvecs,bvals),output_time=True)
#     run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_params + "_L1.nii.gz",dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_MD.nii.gz"),output_time=True)
#     run("fslmaths {} -add {}  -div 2 {} ".format(dti_params + "_L2.nii.gz",dti_params + "_L3.nii.gz",dti_params + "_RD.nii.gz"),output_time=True)

#     copy(dti_params + "_L1.nii.gz",dti_params + "_AD.nii.gz")
#     copy(dti_params + "_FA.nii.gz",fa)

# if args.output_time:
#     printFinish(start_time)
