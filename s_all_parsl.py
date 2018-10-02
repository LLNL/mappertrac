#!/usr/bin/env python3
import argparse
import sys
import os
import multiprocessing
import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers import LocalProvider,SlurmProvider
from libsubmit.channels import LocalChannel,SSHInteractiveLoginChannel
from libsubmit.launchers import SrunLauncher
from glob import glob
# from shutil import *
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,isdir
from os import system,mkdir,remove,environ,makedirs

parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('subject_list',help='Text file with list of subject directories.')
parser.add_argument('output_dir',help='The super-directory that will contain output directories for each subject')
parser.add_argument('--force',help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--output_time',help='Print completion time',action='store_true')
# parser.add_argument('--gpu_host_domain',help='Host to run GPU-enabled jobs',default='pascal.llnl.gov')
# parser.add_argument('--host_username',help='Username on cluster',default='moon15')
args = parser.parse_args()

start_time = printStart()

config = Config(
    executors=[
        IPyParallelExecutor(
            label='local_node',
            provider=LocalProvider(
                init_blocks=multiprocessing.cpu_count(),
                max_blocks=multiprocessing.cpu_count(),
            )
        ),
        IPyParallelExecutor(
            label='pascal_node',
            provider=SlurmProvider(
                'pbatch',
                channel=SSHInteractiveLoginChannel(
                    hostname='pascal.llnl.gov',
                    username='moon15',
                ),
                launcher=SrunLauncher(),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:15:00",
                overrides="#SBATCH -A asccasc"
#SBATCH -o pascal_node_parsl.stdout
# module load cuda/8.0;"""
            ),
            controller=Controller(public_ip=user_opts['public_ip'])
        )
    ]
)

parsl.load(config)

@python_app(executors=["local_node"])
def j1_split_timesteps(input_dir, sdir, force, outputs=[]):
    from subscripts.utilities import run, smart_copy
    from os.path import join,exists
    smart_copy(join(input_dir,"bvecs"),join(sdir,"bvecs"),force)
    smart_copy(join(input_dir,"bvals"),join(sdir,"bvals"),force)
    smart_copy(join(input_dir,"anat.nii.gz"),join(sdir,"T1.nii.gz"),force)
    input_data = join(input_dir, "hardi.nii.gz")
    output_prefix = join(sdir,"data_eddy")
    output_data = join(sdir,"data_eddy.nii.gz")
    if force or not exists(output_data):
        run("fslroi {} {}_ref 0 1".format(input_data, output_prefix))
        run("fslsplit {} {}_tmp".format(input_data, output_prefix))

@python_app(executors=["local_node"])
def j2_timestep_process(sdir, step, force, inputs=[]):
    from subscripts.utilities import run
    from os.path import join,exists
    step_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(step))
    if not exists(step_data):
        # print("Failed to open timestep image {}".format(step_data))
        return
    output_prefix = join(sdir,"data_eddy")
    run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1 >> {1}.ecclog".format(step_data, output_prefix))

@python_app(executors=["local_node"])
def j3_dti_fit(input_dir, sdir, force, inputs=[]):
    from subscripts.utilities import run,smart_copy,exist_all
    from glob import glob
    from os import remove
    from os.path import join,exists
    output_prefix = join(sdir,"data_eddy")
    output_data = join(sdir,"data_eddy.nii.gz")
    timesteps = glob("{}_tmp????.*".format(output_prefix))
    if force or not exists(output_data):
        run("fslmerge -t {} {}".format(output_data, " ".join(timesteps)))
    for i in timesteps:
        remove(i)
    for j in glob("{}_ref*".format(output_prefix)):
        remove(j)

    bet = join(sdir,"data_bet.nii.gz")
    if force or not exists(bet):
        run("bet {} {} -m -f 0.3".format(output_data,bet))

    bvecs = join(sdir,"bvecs")
    bvals = join(sdir,"bvals")
    bet_mask = join(sdir,"data_bet_mask.nii.gz")
    dti_params = join(sdir,"DTIparams")
    dti_L1 = dti_params + "_L1.nii.gz"
    dti_L2 = dti_params + "_L2.nii.gz"
    dti_L3 = dti_params + "_L3.nii.gz"
    dti_MD = dti_params + "_MD.nii.gz"
    dti_RD = dti_params + "_RD.nii.gz"
    dti_MD = dti_params + "_MD.nii.gz"
    dti_AD = dti_params + "_AD.nii.gz"
    dti_FA = dti_params + "_FA.nii.gz"
    FA = join(sdir,"FA.nii.gz")
    if force or not exist_all([dti_L1,dti_L2,dti_L3]):
        run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(output_data,dti_params,bet_mask,bvecs,bvals))
    if force or not exists(dti_MD):
        run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_L1,dti_L2,dti_L3,dti_MD))
    if force or not exists(dti_RD):
        run("fslmaths {} -add {}  -div 2 {} ".format(dti_L2,dti_L3,dti_RD))

    smart_copy(dti_L1,dti_AD,force)
    smart_copy(dti_FA,FA,force)

    ### not used anywhere else, delete?
    # bvecs_rotated = join(sdir,"bvecs_rotated") 
    # eddy_log = join(sdir,"data_eddy.ecclog")
    # run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log))

@python_app(executors=["pascal_node"])
def j4_bedpostx(sdir, force, inputs=[]):
    print("b")
    # import shutil
    # from subscripts.utilities import run,smart_copy,smart_mkdir,exist_all
    # from os.path import join,exists
    # bedpostx = join(sdir,"bedpostx_b1000")
    # bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    # bedpostxResultsEDI = join(sdir,"EDI","bedpostx_b1000.bedpostX")
    # smart_mkdir(bedpostx)
    # smart_mkdir(bedpostxResults)
    # smart_copy(join(sdir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),force)
    # smart_copy(join(sdir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),force)
    # smart_copy(join(sdir,"bvals"),join(bedpostx,"bvals"),force)
    # smart_copy(join(sdir,"bvecs"),join(bedpostx,"bvecs"),force)

    # th1 = join(bedpostxResults, "merged_th1samples")
    # ph1 = join(bedpostxResults, "merged_ph1samples")
    # th2 = join(bedpostxResults, "merged_th2samples")
    # ph2 = join(bedpostxResults, "merged_ph2samples")
    # dyads1 = join(bedpostxResults, "dyads1")
    # dyads2 = join(bedpostxResults, "dyads2")
    # brain_mask = join(bedpostxResults, "nodif_brain_mask")
    # if force or not exist_all([th1,ph1,th2,ph2]):
    #     run("bedpostx_gpu " + bedpostx + " -NJOBS 4")
    # if force or not exists(dyads1):
    #     run("make_dyadic_vectors {} {} {} {}".format(th1,ph1,brain_mask,dyads1))
    # if force or not exists(dyads2):
    #     run("make_dyadic_vectors {} {} {} {}".format(th2,ph2,brain_mask,dyads2))
    # if force or not exists(bedpostxResultsEDI):
    #     if exists(bedpostxResultsEDI):
    #         shutil.rmtree(bedpostxResultsEDI)
    #     shutil.copytree(bedpostxResults, bedpostxResultsEDI)

@python_app
def j5_freesurfer(sdir, inputs=[]):
    pass

@python_app
def j6_freesurfer_postproc(sdir, inputs=[]):
    pass

@python_app
def j7_edi_preproc(sdir, inputs=[]):
    pass

@python_app
def j8_edi(sdir, inputs=[]):
    pass

odir = abspath(args.output_dir)
if not isdir(odir):
    makedirs(odir)
jobs = []
with open(args.subject_list) as f:
    for input_dir in f.readlines():
        input_dir = input_dir.strip()
        input_data = join(input_dir, "hardi.nii.gz")
        sdir = join(odir, basename(input_dir))
        if not isdir(sdir):
            mkdir(sdir)
        num_timesteps = run("fslinfo {} | sed -n -e '/^dim4/p'".format(input_data)).split()[-1]
        if not isInteger(num_timesteps):
            print("Failed to read timesteps from {}".format(input_data))
            continue
        j1_future = j1_split_timesteps(input_dir, sdir, args.force)
        j2_futures = []
        for i in range(int(num_timesteps)):
            j2_future = j2_timestep_process(sdir, i, args.force, inputs=[j1_future])
            j2_futures.append(j2_future)
        j3_future = j3_dti_fit(input_dir, sdir, args.force, inputs=j2_futures)
        # j4_future = j4_bedpostx(sdir, args.force, inputs=[j3_future])
        jobs.append(j3_future)
for job in jobs:
    job.result()
if args.output_time:
    printFinish(start_time)
