#!/usr/bin/env python3
import argparse
import sys
import os
import multiprocessing
import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.providers import LocalProvider,SlurmProvider
from parsl.channels import LocalChannel,SSHInteractiveLoginChannel
from parsl.launchers import SrunLauncher
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,isdir
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
            label='threaded',
            provider=SlurmProvider(
                'pbatch',
                channel=LocalChannel(),
                launcher=SrunLauncher(),
                nodes_per_block=1,
                tasks_per_node=36,
                init_blocks=1,
                max_blocks=1,
                walltime="00:03:00",
                overrides="""#SBATCH -A ccp"""
            ),
        ),
#         IPyParallelExecutor(
#             label='gpu',
#             provider=SlurmProvider(
#                 'pbatch',
#                 channel=LocalChannel(),
#                 launcher=SrunLauncher(),
#                 nodes_per_block=1,
#                 tasks_per_node=1,
#                 init_blocks=1,
#                 max_blocks=1,
#                 walltime="00:15:00",
#                 overrides="""#SBATCH -A ccp
# module load cuda/8.0;"""
#             ),
#         )
    ]
)

parsl.set_stream_logger()
parsl.load(config)

@python_app(executors=["threaded"])
def j1_split_timesteps(input_dir, sdir, force):
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

@python_app(executors=["threaded"])
def j2_timestep_process(sdir, step, force, inputs=[]):
    from subscripts.utilities import run
    from os.path import join,exists
    step_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(step))
    if not exists(step_data):
        # print("Failed to open timestep image {}".format(step_data))
        return
    output_prefix = join(sdir,"data_eddy")
    run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1 >> {1}.ecclog".format(step_data, output_prefix))

@python_app(executors=["threaded"])
def j3_dti_fit(input_dir, sdir, force, inputs=[]):
    from subscripts.utilities import run,smart_copy,smart_run,exist_all
    from glob import glob
    from os import remove
    from os.path import join,exists
    output_prefix = join(sdir,"data_eddy")
    output_data = join(sdir,"data_eddy.nii.gz")
    timesteps = glob("{}_tmp????.*".format(output_prefix))
    smart_run("fslmerge -t {} {}".format(output_data, " ".join(timesteps)), output_data, force)
    for i in timesteps:
        remove(i)
    for j in glob("{}_ref*".format(output_prefix)):
        remove(j)

    bet = join(sdir,"data_bet.nii.gz")
    smart_run("bet {} {} -m -f 0.3".format(output_data,bet), bet, force)

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
    smart_run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_L1,dti_L2,dti_L3,dti_MD), dti_MD, force)
    smart_run("fslmaths {} -add {}  -div 2 {} ".format(dti_L2,dti_L3,dti_RD), dti_RD, force)

    smart_copy(dti_L1,dti_AD,force)
    smart_copy(dti_FA,FA,force)

    ### not used anywhere else, delete?
    # bvecs_rotated = join(sdir,"bvecs_rotated") 
    # eddy_log = join(sdir,"data_eddy.ecclog")
    # run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log))

@python_app(executors=["threaded"])
def j4_bedpostx(sdir, force, inputs=[]):
    import shutil
    from subscripts.utilities import run,smart_copy,smart_mkdir,smart_run,exist_all
    from os.path import join,exists
    bedpostx = join(sdir,"bedpostx_b1000")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    bedpostxResultsEDI = join(sdir,"EDI","bedpostx_b1000.bedpostX")
    smart_mkdir(bedpostx)
    smart_mkdir(bedpostxResults)
    smart_copy(join(sdir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),force)
    smart_copy(join(sdir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),force)
    smart_copy(join(sdir,"bvals"),join(bedpostx,"bvals"),force)
    smart_copy(join(sdir,"bvecs"),join(bedpostx,"bvecs"),force)

    th1 = join(bedpostxResults, "merged_th1samples")
    ph1 = join(bedpostxResults, "merged_ph1samples")
    th2 = join(bedpostxResults, "merged_th2samples")
    ph2 = join(bedpostxResults, "merged_ph2samples")
    dyads1 = join(bedpostxResults, "dyads1")
    dyads2 = join(bedpostxResults, "dyads2")
    brain_mask = join(bedpostxResults, "nodif_brain_mask")
    if force or not exist_all([th1,ph1,th2,ph2]):
        run("bedpostx_gpu " + bedpostx + " -NJOBS 4")
    smart_run("make_dyadic_vectors {} {} {} {}".format(th1,ph1,brain_mask,dyads1), dyads1, force)
    smart_run("make_dyadic_vectors {} {} {} {}".format(th2,ph2,brain_mask,dyads2), dyads2, force)
    if force or not exists(bedpostxResultsEDI):
        if exists(bedpostxResultsEDI):
            shutil.rmtree(bedpostxResultsEDI)
        shutil.copytree(bedpostxResults, bedpostxResultsEDI)

@python_app(executors=["threaded"])
def j5_freesurfer(sdir, force, use_gpu=True, inputs=[]):
    from subscripts.utilities import run,smart_mkdir
    from os import environ
    from os.path import join,exists,islink,split
    T1 = join(sdir,"T1.nii.gz")
    subject = split(sdir)[1]
    environ['SUBJECTS_DIR'] = split(sdir)[0]
    if use_gpu:
        environ['CUDA_LIB_DIR'] = environ['CUDA_5_LIB_DIR']
        environ['LD_LIBRARY_PATH'] = "{}:{}".format(environ['CUDA_LIB_DIR'],environ['LD_LIBRARY_PATH'])
    smart_mkdir(join(sdir,"mri"))
    smart_mkdir(join(sdir,"mri","orig"))

    mri_out = join(sdir,"mri","orig","001.mgz")
    smart_run("mri_convert {} {}".format(T1,mri_out), mri_out, force)

    if islink(join(environ['SUBJECTS_DIR'],"fsaverage")):
        run("unlink {}".format(join(environ['SUBJECTS_DIR'],"fsaverage")))

    if force or not exists(join(sdir,"mri","aparc+aseg.mgz")):
        if use_gpu:
            run("recon-all -s {} -all -no-isrunning -use-gpu -parallel -openmp {}".format(subject, multiprocessing.cpu_count()))
        else:
            run("recon-all -s {} -all -no-isrunning -parallel -openmp {}".format(subject, multiprocessing.cpu_count()))

@python_app(executors=["threaded"])
def j6_freesurfer_postproc(sdir, force, inputs=[]):
    from subscripts.utilities import run,smart_copy,smart_run,smart_mkdir,smart_remove,exist_all
    from subscripts.maskseeds import maskseeds,saveallvoxels
    from glob import glob
    from os import remove
    from os.path import join,exists,split,splitext
    subject = split(sdir)[1]
    T1 = join(sdir,"T1.nii.gz")
    FA = join(sdir,"FA.nii.gz")
    aseg = join(sdir,"aseg.nii.gz")
    bs = join(sdir,"bs.nii.gz")
    FA2T1 = join(sdir,"FA2T1.mat")
    T12FA = join(sdir,"T12FA.mat")
    cort_label_dir = join(sdir,"label_cortical")
    cort_vol_dir = join(sdir,"volumes_cortical")
    cort_vol_dir_out = vol_dir + "_s2fa"
    subcort_vol_dir = join(sdir,"volumes_subcortical")
    subcort_vol_dir_out = sub_vol_dir + "_s2fa"
    terminationmask = join(sdir,"terminationmask.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    intersection = join(sdir,"intersection.nii.gz")
    EDI = join(sdir,"EDI")
    EDI_allvols = join(EDI,"allvols")

    run("mri_convert {} {} ".format(join(sdir,"mri","brain.mgz"),T1))
    smart_run("flirt -in {} -ref {} -omat {}".format(FA,T1,FA2T1), FA2T1, force)
    smart_run("convert_xfm -omat {} -inverse {}".format(T12FA,FA2T1), T12FA, force)

    smart_mkdir(cort_label_dir)
    if force or not exists(join(cort_label_dir,"rh.temporalpole.label")):
        run("mri_annotation2label --subject {} --hemi rh --annotation aparc --outdir {}".format(subject, cort_label_dir))
    if force or not exists(join(cort_label_dir,"lh.temporalpole.label")):    
        run("mri_annotation2label --subject {} --hemi lh --annotation aparc --outdir {}".format(subject, cort_label_dir))

    smart_mkdir(cort_vol_dir)
    for label in glob(join(cort_label_dir,"*.label")):
        vol_dir = join(cort_vol_dir, splitext(split(label)[1])[0] + ".nii.gz")
        smart_run("mri_label2vol --label {} --temp {} --identity --o {}".format(label,T1,vol_dir), vol_dir, force)

    run("mri_convert {} {}".format(join(sdir,"mri","aseg.mgz"),aseg), aseg, force)

    smart_mkdir(subcort_vol_dir)
    for line in open(args.subcortical_index,"r").readlines():
        num = line.split(":")[0].lstrip().rstrip()
        area = line.split(":")[1].lstrip().rstrip()
        area_out = join(subcort_vol_dir,area + ".nii.gz")
        print("Processing " + area + ".nii.gz")
        smart_run("fslmaths {} -uthr {} -thr {} -bin {}".format(aseg,num,num,area_out), area_out, force)

    smart_mkdir(cort_vol_dir_out)
    for volume in glob(join(cort_vol_dir,"*.nii.gz")):
        out_vol = join(cort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        print("Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        if force or not exists(out_vol):
            run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA))
            run("fslmaths {} -thr 0.2 -bin {} ".format(out_vol,out_vol))

    smart_mkdir(subcort_vol_dir_out)
    for volume in glob(join(subcort_vol_dir,"*.nii.gz")):
        out_vol = join(subcort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        print("Processing ",split(volume)[1]," -> ",split(out_vol)[1])
        if force or not exists(out_vol):
            run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA))
            run("fslmaths {} -thr 0.2 -bin {} ".format(out_vol,out_vol))

    run("fslmaths {} -mul 0 {}".format(FA,bs)) # For now we fake a bs.nii.gz file
    maskseeds(sdir,join(cort_vol_dir + "_s2fa"),join(cort_vol_dir + "_s2fa_m"),0.05,1,1)
    maskseeds(sdir,join(subcort_vol_dir + "_s2fa"),join(subcort_vol_dir + "_s2fa_m"),0.05,0.4,0.4)

    saveallvoxels(sdir,join(cort_vol_dir + "_s2fa_m"),join(subcort_vol_dir + "_s2fa_m"),allvoxelscortsubcort,force)

    smart_remove(terminationmask)
    run("fslmaths {} -uthr .15 {}".format(FA, terminationmask))
    run("fslmaths {} -add {} {}".format(terminationmask, bs, terminationmask))
    run("fslmaths {} -bin {}".format(terminationmask, terminationmask))
    run("fslmaths {} -mul {} {}".format(terminationmask, allvoxelscortsubcort, intersection))
    run("fslmaths {} -sub {} {}".format(terminationmask, intersection, terminationmask))
    run("fslmaths {} -add {} -add {} {}".format(bs,
                                                join(subcort_vol_dir + "_s2fa_m","lh_thalamus_s2fa.nii.gz"),
                                                join(subcort_vol_dir + "_s2fa_m","rh_thalamus_s2fa.nii.gz"),
                                                join(sdir,"exlusion_bsplusthalami.nii.gz")))
    smart_mkdir(EDI)
    smart_mkdir(EDI_allvols)
    for file in glob(join(sdir,"volumes_cortical_s2fa","*")):
        smart_copy(file,EDI_allvols,force)

    for file in glob(join(sdir,"volumes_subcortical_s2fa","*")):
        smart_copy(file,EDI_allvols,force)
    smart_copy(bs,EDI,force)
    smart_copy(terminationmask,EDI,force)
    smart_copy(join(sdir,"exlusion_bsplusthalami.nii.gz"),EDI,force)
    smart_copy(allvoxelscortsubcort,EDI,force)

@python_app(executors=["threaded"])
def j7_edi_preproc(sdir, force, inputs=[]):
    pass

@python_app(executors=["threaded"])
def j8_edi_oneway(sdir, force, inputs=[]):
    pass

@python_app(executors=["threaded"])
def j9_edi_consensus(sdir, force, inputs=[]):
    pass

@python_app(executors=["threaded"])
def reflect(input):
    return input

odir = abspath(args.output_dir)
if not isdir(odir):
    makedirs(odir)
jobs = []
with open(args.subject_list) as f:
    for input_dir in f.readlines():
        # jobs.append(reflect(input_dir))
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
        # j2_futures = []
        # for i in range(int(num_timesteps)):
        #     j2_future = j2_timestep_process(sdir, i, args.force, inputs=[j1_future])
        #     j2_futures.append(j2_future)
        # j3_future = j3_dti_fit(input_dir, sdir, args.force, inputs=j2_futures)
        # j4_future = j4_bedpostx(sdir, args.force, inputs=[j3_future])
        # j5_future = j5_freesurfer(sdir, args.force, inputs=[j3_future])
        # j6_future = j6_freesurfer_postproc(sdir, args.force, inputs=[j5_future])
        # j7_future = j7_edi_preproc(sdir, args.force, inputs=[j4_future, j6_future])
        jobs.append(j1_future)
for job in jobs:
    job.result()
if args.output_time:
    printFinish(start_time)
