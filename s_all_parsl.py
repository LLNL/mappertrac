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
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,isdir
from os import system,mkdir,remove,environ,makedirs
from math import floor

parser = argparse.ArgumentParser(description='Generate connectome data',usage="%(prog)s subject_list output_dir [--force] [--edge_list EDGE_LIST] [s1] [s2a] [s2b] [s3] [s4]\n")
parser.add_argument('subject_list',help='Text file with list of subject directories.')
parser.add_argument('output_dir',help='The super-directory that will contain output directories for each subject')
parser.add_argument('script', choices=['s1','s2a','s2b','s2b_gpu','s3'], type=str.lower, default='s1', help='Script to run across subjects')
parser.add_argument('--force',help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--edge_list',help='Edges processed by probtrackx, in s3_ediPreproc',default=join("lists","listEdgesEDI.txt"))
parser.add_argument('--log_dir',help='Directory containing log output',default=join("parsl_logs"))
args = parser.parse_args()

s1 = args.script == 's1'
s2a = args.script == 's2a'
s2b = args.script == 's2b'
s2b_gpu = args.script == 's2b_gpu'
s3 = args.script == 's3'

# Use only two Slurm executors for now, to prevent requesting unnecessary resources
# 1. Local uses the initially running node's cpus, so we don't waste them.
# 2. Batch requests additional nodes on the same cluster.
# Parsl will distribute tasks between both executors.

def get_executors(_tasks_per_node, _nodes_per_block, _max_blocks, _walltime, _overrides):
    return [IPyParallelExecutor(label='local',
                provider=LocalProvider(
                init_blocks=_tasks_per_node,
                max_blocks=_tasks_per_node)),
            IPyParallelExecutor(label='batch',
                provider=SlurmProvider('pbatch',
                launcher=SrunLauncher(),
                nodes_per_block=_nodes_per_block,
                tasks_per_node=_tasks_per_node,
                init_blocks=1,
                max_blocks=_max_blocks,
                walltime=_walltime,
                overrides=_overrides))
        ]
def get_executor_labels(_nodes_per_block, _max_blocks):
    labels = ['local']
    for i in range(_nodes_per_block * _max_blocks):
        labels.append('batch')
    return labels
    # return ['batch']
    # return ['local']

num_cores = int(floor(multiprocessing.cpu_count() / 2))
tasks_per_node = 1
nodes_per_block = 4
max_blocks = 1
walltime = "03:00:00"
slurm_override = """#SBATCH -A ccp"""

if s1:
    tasks_per_node = num_cores
    nodes_per_block = 4
    max_blocks = 1
    walltime = "03:00:00"
elif s2a:
    tasks_per_node = 1
    nodes_per_block = 4
    max_blocks = 2
    walltime = "03:00:00"
    slurm_override = """#SBATCH -A ccp
module load cuda/8.0;"""
elif s2b:
    tasks_per_node = 1
    nodes_per_block = 8
    max_blocks = 2
    walltime = "23:59:00"
elif s2b_gpu:
    tasks_per_node = 1
    nodes_per_block = 4
    max_blocks = 2
    walltime = "06:00:00"
elif s3:
    tasks_per_node = num_cores
    nodes_per_block = 8
    max_blocks = 1
    walltime = "03:00:00"
executors = get_executors(tasks_per_node, nodes_per_block, max_blocks, walltime, slurm_override)
executor_labels = get_executor_labels(nodes_per_block, max_blocks)
config = Config(executors=executors)

start_time = printStart()
parsl.set_stream_logger()
parsl.load(config)

odir = abspath(args.output_dir)
if not isdir(odir):
    makedirs(odir)
jobs = []
edges = []

if s1:
    @python_app(executors=executor_labels)
    def s1_1_split_timesteps(input_dir, sdir, stdout, force):
        from subscripts.utilities import run,smart_copy,writeStart,writeFinish,writeOutput
        from os.path import join,exists,basename
        return
        # start_time = writeStart(stdout, "s1_1_split_timesteps")
        smart_copy(join(input_dir,"bvecs"),join(sdir,"bvecs"),force)
        smart_copy(join(input_dir,"bvals"),join(sdir,"bvals"),force)
        smart_copy(join(input_dir,"anat.nii.gz"),join(sdir,"T1.nii.gz"),force)
        input_data = join(input_dir, "hardi.nii.gz")
        output_prefix = join(sdir,"data_eddy")
        output_data = join(sdir,"data_eddy.nii.gz")
        if force or not exists(output_data):
            run("fslroi {} {}_ref 0 1".format(input_data, output_prefix), write_output=stdout)
            run("fslsplit {} {}_tmp".format(input_data, output_prefix), write_output=stdout)
        # writeFinish(stdout, start_time, "s1_1_split_timesteps")

    @python_app(executors=executor_labels)
    def s1_2_timestep_process(sdir, step, stdout, force, inputs=[]):
        from subscripts.utilities import run
        from os.path import join,exists
        return
        step_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(step))
        if not exists(step_data):
            # print("Failed to open timestep image {}".format(step_data))
            return
        output_prefix = join(sdir,"data_eddy")
        run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1 >> {1}.ecclog".format(step_data, output_prefix))

    @python_app(executors=executor_labels)
    def s1_3_dti_fit(input_dir, sdir, stdout, force, inputs=[]):
        from subscripts.utilities import run,smart_copy,smart_run,exist_all,writeStart,writeFinish,writeOutput
        from glob import glob
        from os import remove
        from os.path import join,exists,basename
        return
        # start_time = writeStart(stdout, "s1_3_dti_fit")
        output_prefix = join(sdir,"data_eddy")
        output_data = join(sdir,"data_eddy.nii.gz")
        timesteps = glob("{}_tmp????.*".format(output_prefix))
        smart_run("fslmerge -t {} {}".format(output_data, " ".join(timesteps)), output_data, force, write_output=stdout)
        for i in timesteps:
            remove(i)
        for j in glob("{}_ref*".format(output_prefix)):
            remove(j)

        bet = join(sdir,"data_bet.nii.gz")
        smart_run("bet {} {} -m -f 0.3".format(output_data,bet), bet, force, write_output=stdout)

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
            run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(output_data,dti_params,bet_mask,bvecs,bvals), write_output=stdout)
        smart_run("fslmaths {} -add {} -add {} -div 3 {} ".format(dti_L1,dti_L2,dti_L3,dti_MD), dti_MD, force, write_output=stdout)
        smart_run("fslmaths {} -add {}  -div 2 {} ".format(dti_L2,dti_L3,dti_RD), dti_RD, force, write_output=stdout)

        smart_copy(dti_L1,dti_AD,force)
        smart_copy(dti_FA,FA,force)

        ### not used anywhere else, delete?
        # bvecs_rotated = join(sdir,"bvecs_rotated") 
        # eddy_log = join(sdir,"data_eddy.ecclog")
        # run("fdt_rotate_bvecs {} {} {}".format(bvecs,bvecs_rotated,eddy_log))

        # writeFinish(stdout, start_time, "s1_3_dti_fit")

elif s2a:
    @python_app(executors=['batch'])
    def s2a_bedpostx(sdir, stdout, force):
        import shutil
        from subscripts.utilities import run,smart_copy,smart_mkdir,smart_run,exist_all,writeStart,writeFinish,writeOutput
        from os.path import join,exists,basename
        start_time = writeStart(stdout, "s2a_bedpostx")
        bedpostx = join(sdir,"bedpostx_b1000")
        bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
        # bedpostxResultsEDI = join(sdir,"EDI","bedpostx_b1000.bedpostX")
        th1 = join(bedpostxResults, "merged_th1samples")
        ph1 = join(bedpostxResults, "merged_ph1samples")
        th2 = join(bedpostxResults, "merged_th2samples")
        ph2 = join(bedpostxResults, "merged_ph2samples")
        dyads1 = join(bedpostxResults, "dyads1")
        dyads2 = join(bedpostxResults, "dyads2")
        brain_mask = join(bedpostxResults, "nodif_brain_mask")
        do_run = force or not exist_all([th1,ph1,th2,ph2],".nii.gz") # we re-run many commands for fault tolerance
        if not do_run:
            writeOutput(stdout,"Already finished bedpostx in {}. Use --force argument to re-compute.".format(bedpostxResults))
        elif exists(bedpostxResults):
            shutil.rmtree(bedpostxResults)
        smart_mkdir(bedpostx)
        smart_mkdir(bedpostxResults)
        smart_copy(join(sdir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),force)
        smart_copy(join(sdir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),force)
        smart_copy(join(sdir,"bvals"),join(bedpostx,"bvals"),force)
        smart_copy(join(sdir,"bvecs"),join(bedpostx,"bvecs"),force)

        if do_run:
            run("bedpostx_gpu " + bedpostx + " -NJOBS 4", write_output=stdout)
        smart_run("make_dyadic_vectors {} {} {} {}".format(th1,ph1,brain_mask,dyads1), dyads1, force, write_output=stdout)
        smart_run("make_dyadic_vectors {} {} {} {}".format(th2,ph2,brain_mask,dyads2), dyads2, force, write_output=stdout)
        # if force or not exists(bedpostxResultsEDI):
        #     if exists(bedpostxResultsEDI):
        #         shutil.rmtree(bedpostxResultsEDI)
        #     shutil.copytree(bedpostxResults, bedpostxResultsEDI)
        writeFinish(stdout, start_time, "s2a_bedpostx")

elif s2b or s2b_gpu:
    @python_app(executors=['batch'])
    def s2b_1_freesurfer(sdir, use_gpu, stdout, force):
        import multiprocessing
        from subscripts.utilities import run,smart_mkdir,smart_run,writeStart,writeFinish,writeOutput
        from os import environ
        from os.path import join,exists,islink,split,basename
        start_time = writeStart(stdout, "s2b_1_freesurfer")

        T1 = join(sdir,"T1.nii.gz")
        mri_out = join(sdir,"mri","orig","001.mgz")
        subject = split(sdir)[1]
        environ['SUBJECTS_DIR'] = split(sdir)[0]
        num_cores = multiprocessing.cpu_count()

        if use_gpu:
            if not 'CUDA_5_LIB_DIR' in environ:
                writeOutput(stdout, "Error: Environment variable CUDA_5_LIB_DIR not set. Please install CUDA 5 to use Freesurfer GPU functions.")
                return
            environ['CUDA_LIB_DIR'] = environ['CUDA_5_LIB_DIR']
            environ['LD_LIBRARY_PATH'] = "{}:{}".format(environ['CUDA_LIB_DIR'],environ['LD_LIBRARY_PATH'])
        smart_mkdir(join(sdir,"mri"))
        smart_mkdir(join(sdir,"mri","orig"))

        smart_run("mri_convert {} {}".format(T1,mri_out), mri_out, force, write_output=stdout)

        if islink(join(environ['SUBJECTS_DIR'],"fsaverage")):
            run("unlink {}".format(join(environ['SUBJECTS_DIR'],"fsaverage")), write_output=stdout)

        if force or not exists(join(sdir,"mri","aparc+aseg.mgz")):
            writeOutput(stdout, "Running Freesurfer with {} cores".format(num_cores))
            if use_gpu:
                run("recon-all -s {} -all -no-isrunning -use-gpu -parallel -openmp {}".format(subject, num_cores), write_output=stdout)
            else:
                run("recon-all -s {} -all -no-isrunning -parallel -openmp {}".format(subject, num_cores), write_output=stdout)

        writeFinish(stdout, start_time, "s2b_1_freesurfer")

    @python_app(executors=executor_labels)
    def s2b_2_freesurfer_postproc(sdir, stdout, force, inputs=[]):
        from subscripts.utilities import run,smart_copy,smart_run,smart_mkdir,smart_remove,exist_all,writeStart,writeFinish,writeOutput
        from subscripts.maskseeds import maskseeds,saveallvoxels
        from glob import glob
        from os import remove
        from os.path import join,exists,split,splitext,basename
        start_time = writeStart(stdout, "s2b_2_freesurfer_postproc")

        subject = split(sdir)[1]
        T1 = join(sdir,"T1.nii.gz")
        FA = join(sdir,"FA.nii.gz")
        aseg = join(sdir,"aseg.nii.gz")
        bs = join(sdir,"bs.nii.gz")
        FA2T1 = join(sdir,"FA2T1.mat")
        T12FA = join(sdir,"T12FA.mat")
        cort_label_dir = join(sdir,"label_cortical")
        cort_vol_dir = join(sdir,"volumes_cortical")
        cort_vol_dir_out = cort_vol_dir + "_s2fa"
        subcort_vol_dir = join(sdir,"volumes_subcortical")
        subcort_vol_dir_out = subcort_vol_dir + "_s2fa"
        terminationmask = join(sdir,"terminationmask.nii.gz")
        allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
        intersection = join(sdir,"intersection.nii.gz")
        exclusion_bsplusthalami = join(sdir,"exclusion_bsplusthalami.nii.gz")
        subcortical_index = join("lists","subcorticalIndex.txt")
        EDI = join(sdir,"EDI")
        EDI_allvols = join(EDI,"allvols")
        do_run = force or not exists(exclusion_bsplusthalami) # we re-run many commands for fault tolerance
        if not do_run:
            writeOutput(stdout, "Already finished freesurfer in {}. Use --force argument to re-compute.".format(sdir))
        else:
            run("mri_convert {} {} ".format(join(sdir,"mri","brain.mgz"),T1), write_output=stdout)
        smart_run("flirt -in {} -ref {} -omat {}".format(FA,T1,FA2T1), FA2T1, force)
        smart_run("convert_xfm -omat {} -inverse {}".format(T12FA,FA2T1), T12FA, force)

        smart_mkdir(cort_label_dir)
        if force or not exists(join(cort_label_dir,"rh.temporalpole.label")):
            run("mri_annotation2label --subject {} --hemi rh --annotation aparc --outdir {}".format(subject, cort_label_dir), write_output=stdout)
        if force or not exists(join(cort_label_dir,"lh.temporalpole.label")):    
            run("mri_annotation2label --subject {} --hemi lh --annotation aparc --outdir {}".format(subject, cort_label_dir), write_output=stdout)

        smart_mkdir(cort_vol_dir)
        for label in glob(join(cort_label_dir,"*.label")):
            vol_file = join(cort_vol_dir, splitext(split(label)[1])[0] + ".nii.gz")
            smart_run("mri_label2vol --label {} --temp {} --identity --o {}".format(label,T1,vol_file), vol_file, force, write_output=stdout)

        if do_run:
            run("mri_convert {} {}".format(join(sdir,"mri","aseg.mgz"),aseg), write_output=stdout)

        smart_mkdir(subcort_vol_dir)
        for line in open(subcortical_index,"r").readlines():
            num = line.split(":")[0].lstrip().rstrip()
            area = line.split(":")[1].lstrip().rstrip()
            area_out = join(subcort_vol_dir,area + ".nii.gz")
            writeOutput(stdout, "Processing " + area + ".nii.gz")
            smart_run("fslmaths {} -uthr {} -thr {} -bin {}".format(aseg,num,num,area_out), area_out, force, write_output=stdout)

        smart_mkdir(cort_vol_dir_out)
        for volume in glob(join(cort_vol_dir,"*.nii.gz")):
            out_vol = join(cort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
            writeOutput(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
            if force or not exists(out_vol):
                run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), write_output=stdout)
                run("fslmaths {} -thr 0.2 -bin {} ".format(out_vol,out_vol), write_output=stdout)

        smart_mkdir(subcort_vol_dir_out)
        for volume in glob(join(subcort_vol_dir,"*.nii.gz")):
            out_vol = join(subcort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
            writeOutput(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
            if force or not exists(out_vol):
                run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), write_output=stdout)
                run("fslmaths {} -thr 0.2 -bin {}".format(out_vol,out_vol), write_output=stdout)

        if do_run:
            run("fslmaths {} -mul 0 {}".format(FA,bs), write_output=stdout) # For now we fake a bs.nii.gz file
            maskseeds(sdir,join(cort_vol_dir + "_s2fa"),join(cort_vol_dir + "_s2fa_m"),0.05,1,1)
            maskseeds(sdir,join(subcort_vol_dir + "_s2fa"),join(subcort_vol_dir + "_s2fa_m"),0.05,0.4,0.4)
            saveallvoxels(sdir,join(cort_vol_dir + "_s2fa_m"),join(subcort_vol_dir + "_s2fa_m"),allvoxelscortsubcort,force)
            smart_remove(terminationmask)
            run("fslmaths {} -uthr .15 {}".format(FA, terminationmask), write_output=stdout)
            run("fslmaths {} -add {} {}".format(terminationmask, bs, terminationmask), write_output=stdout)
            run("fslmaths {} -bin {}".format(terminationmask, terminationmask), write_output=stdout)
            run("fslmaths {} -mul {} {}".format(terminationmask, allvoxelscortsubcort, intersection), write_output=stdout)
            run("fslmaths {} -sub {} {}".format(terminationmask, intersection, terminationmask), write_output=stdout)
            run("fslmaths {} -add {} -add {} {}".format(bs,
                                                        join(subcort_vol_dir + "_s2fa_m","lh_thalamus_s2fa.nii.gz"),
                                                        join(subcort_vol_dir + "_s2fa_m","rh_thalamus_s2fa.nii.gz"),
                                                        exclusion_bsplusthalami), write_output=stdout)
        smart_mkdir(EDI)
        smart_mkdir(EDI_allvols)
        for file in glob(join(sdir,"volumes_cortical_s2fa","*")):
            smart_copy(file,EDI_allvols,force)
        for file in glob(join(sdir,"volumes_subcortical_s2fa","*")):
            smart_copy(file,EDI_allvols,force)
        # smart_copy(bs,EDI,force)
        # smart_copy(terminationmask,EDI,force)
        # smart_copy(exclusion_bsplusthalami,EDI,force)
        # smart_copy(allvoxelscortsubcort,EDI,force)

        writeFinish(stdout, start_time, "s2b_2_freesurfer_postproc")

elif s3:
    @python_app(executors=executor_labels)
    def s3_1_probtrackx(sdir, a, b, stdout, force):
        import time
        from subscripts.utilities import run,smart_remove,smart_mkdir,writeStart,writeFinish,writeOutput,getTimeString
        from os.path import exists,join,splitext,abspath,split,basename
        # start_time = writeStart(stdout, "s3_1_probtrackx")
        start_time = time.time()

        EDI_allvols = join(sdir,"EDI","allvols")
        a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
        b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
        a_to_b = "{}to{}".format(a, b)
        a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
        pbtk_dir = join(sdir,"EDI","PBTKresults")
        a_to_b_file = join(pbtk_dir,a_to_b_formatted)
        waypoints = join(sdir,"tmp","tmp_waypoint_{}.txt".format(a_to_b))
        exclusion = join(sdir,"tmp","tmp_exclusion_{}.nii.gz".format(a_to_b))
        termination = join(sdir,"tmp","tmp_termination_{}.nii.gz".format(a_to_b))
        bs = join(sdir,"bs.nii.gz")
        allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
        terminationmask = join(sdir,"terminationmask.nii.gz")
        bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
        merged = join(bedpostxResults,"merged")
        nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")

        if not exists(a_file) or not exists(b_file):
            writeOutput(stdout, "Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file))
            return

        do_run = force or not exists(a_to_b_file)
        if not do_run:
            writeOutput(stdout, "Already calculated edge {}. Use --force to re-compute.".format(a_to_b))
            return
        smart_remove(a_to_b_file)
        smart_mkdir(pbtk_dir)
        writeOutput(stdout, "Running subproc: {}".format(a_to_b))

        run("fslmaths {} -sub {} {}".format(allvoxelscortsubcort, a_file, exclusion), write_output=stdout)
        run("fslmaths {} -sub {} {}".format(exclusion, b_file, exclusion), write_output=stdout)
        run("fslmaths {} -add {} {}".format(exclusion, bs, exclusion), write_output=stdout)
        run("fslmaths {} -add {} {}".format(terminationmask, b_file, termination), write_output=stdout)

        with open((waypoints),"w") as f:
            f.write(b_file + "\n")

        arguments = (" -x {} ".format(a_file)
            + " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000"
            + " --waypoints={} --avoid={} --stop={}".format(waypoints, exclusion, termination)
            + " --forcedir --opd"
            + " -s {}".format(merged)
            + " -m {}".format(nodif_brain_mask)
            + " --dir={}".format(pbtk_dir)
            + " --out={}".format(a_to_b_formatted)
            + " --omatrix1"
        )
        run("probtrackx2" + arguments, write_output=stdout)
        writeOutput(stdout, "Finished s_3_1 subproc: {}, subject {}\nTime: {} (h:m:s)".format(a_to_b, basename(sdir), getTimeString(time.time() - start_time)))

    @python_app(executors=executor_labels)
    def s3_2_edi_consensus(sdir, a, b, stdout, force, inputs=[]):
        import time
        from subscripts.utilities import run,isFloat,smart_mkdir,smart_remove,writeStart,writeFinish,writeOutput,getTimeString
        from os.path import exists,join,splitext,abspath,split,basename
        # start_time = writeStart(stdout, "s3_2_edi_consensus")
        start_time = time.time()

        pbtk_dir = join(sdir,"EDI","PBTKresults")
        a_to_b = "{}to{}".format(a, b)
        a_to_b_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(a,b))
        b_to_a_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(b,a))
        smart_mkdir(join(pbtk_dir,"twoway_consensus_edges"))
        consensus = join(pbtk_dir,"twoway_consensus_edges",a_to_b)
        do_run = force or not exists(consensus)
        if not do_run:
            writeOutput(stdout, "Already calculated edge consensus {}. Use --force to re-compute.".format(consensus))
            return
        amax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(a_to_b_file), print_output=False, working_dir=pbtk_dir).strip()
        if not isFloat(amax):
            writeOutput(stdout, "Error: fslstats on " + a_to_b_file + " returns invalid value")
            return
        amax = int(float(amax))

        bmax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(b_to_a_file), print_output=False, working_dir=pbtk_dir).strip()
        if not isFloat(bmax):
            writeOutput(stdout, "Error: fslstats on " + b_to_a_file + " returns invalid value")
            return
        bmax = int(float(bmax))
        if amax > 0 and bmax > 0:
            tmp1 = join(pbtk_dir, "{}to{}_tmp1.nii.gz".format(a, b))
            tmp2 = join(pbtk_dir, "{}to{}_tmp2.nii.gz".format(b, a))
            run("fslmaths {} -thrP 5 -bin {}".format(a_to_b_file, tmp1), working_dir=pbtk_dir, write_output=stdout)
            run("fslmaths {} -thrP 5 -bin {}".format(b_to_a_file, tmp2), working_dir=pbtk_dir, write_output=stdout)
            run("fslmaths {} -add {} -thr 1 -bin {}".format(tmp1, tmp2, consensus), working_dir=pbtk_dir, write_output=stdout)
            smart_remove(tmp1)
            smart_remove(tmp2)
        else:
            with open(join(pbtk_dir, "zerosl.txt"), 'a') as log:
                log.write("For edge {}:\n".format(a_to_b))
                log.write("{} is thresholded to {}\n".format(a, amax))
                log.write("{} is thresholded to {}\n".format(b, bmax))

        writeOutput(stdout, "Finished s_3_2 subproc: {}, subject {}\nTime: {} (h:m:s)".format(a_to_b, basename(sdir), getTimeString(time.time() - start_time)))

    @python_app(executors=executor_labels)
    def s3_3_edi_combine(sdir, consensus_edges, stdout, force, inputs=[]):
        from subscripts.utilities import run,smart_copy,smart_mkdir,writeStart,writeFinish,writeOutput
        from os.path import exists,join,split
        start_time = writeStart(stdout, "s3_3_edi_combine")
        pbtk_dir = join(sdir,"EDI","PBTKresults")
        edi_maps = join(sdir,"EDI","EDImaps")
        smart_mkdir(edi_maps)
        total = join(edi_maps,"FAtractsumsTwoway.nii.gz")

        for a_to_b in consensus_edges:
            consensus = join(pbtk_dir,"twoway_consensus_edges",a_to_b+".nii.gz")
            if not exists(consensus):
                writeOutput(stdout,"{} has been thresholded. See {} for details".format(a_to_b, join(pbtk_dir, "zerosl.txt")))
                continue
            if not exists(total):
                smart_copy(consensus, total)
            else:    
                run("fslmaths {0} -add {1} {1}".format(consensus, total), write_output=stdout)
        writeFinish(stdout, start_time, "s3_3_edi_combine")

log_dir = join(odir, args.log_dir)
smart_mkdir(log_dir)
with open(args.subject_list) as f:
    for input_dir in f.readlines():
        input_dir = input_dir.strip()
        input_data = join(input_dir, "hardi.nii.gz")
        sdir = join(odir, basename(input_dir))
        stdout = join(log_dir, basename(sdir) + ".stdout")
        if not isdir(sdir):
            mkdir(sdir)
        if s1:
            num_timesteps = run("fslinfo {} | sed -n -e '/^dim4/p'".format(input_data)).split()[-1]
            if not isInteger(num_timesteps):
                print("Failed to read timesteps from {}".format(input_data))
                continue
            s1_1_future = s1_1_split_timesteps(input_dir, sdir, stdout, args.force)
            s1_2_futures = []
            for i in range(int(num_timesteps)):
                s1_2_future = s1_2_timestep_process(sdir, i, stdout, args.force, inputs=[s1_1_future])
                s1_2_futures.append(s1_2_future)
            s1_3_future = s1_3_dti_fit(input_dir, sdir, stdout, args.force, inputs=s1_2_futures)
            jobs.append(s1_1_future)
        elif s2a:
            s2a_future = s2a_bedpostx(sdir, stdout, args.force)
            jobs.append(s2a_future)
        elif s2b or s2b_gpu:
            s2b_1_future = s2b_1_freesurfer(sdir, s2b_gpu, stdout, args.force)
            s2b_2_future = s2b_2_freesurfer_postproc(sdir, stdout, args.force, inputs=[s2b_1_future])
            jobs.append(s2b_2_future)
        elif s3:
            s3_2_futures = []
            oneway_edges = []
            consensus_edges = []
            with open(args.edge_list) as f:
                for edge in f.readlines():
                    if edge.isspace():
                        continue
                    edge = edge.replace("_s2fa", "")
                    a, b = edge.strip().split(',', 1)
                    a_to_b = "{}to{}".format(a, b)
                    b_to_a = "{}to{}".format(b, a)
                    if not a_to_b in oneway_edges and not b_to_a in oneway_edges:
                        s3_1_future_ab = s3_1_probtrackx(sdir, a, b, stdout, args.force)
                        s3_1_future_ba = s3_1_probtrackx(sdir, b, a, stdout, args.force)
                        s3_2_future = s3_2_edi_consensus(sdir, a, b, stdout, args.force, inputs=[s3_1_future_ab, s3_1_future_ba])
                        s3_2_futures.append(s3_2_future)
                        oneway_edges.append(a_to_b)
                        oneway_edges.append(b_to_a)
                        consensus_edges.append(a_to_b)
            s3_3_future = s3_3_edi_combine(sdir, consensus_edges, stdout, args.force, inputs=s3_2_futures)
            jobs.append(s3_3_future)

for job in jobs:
    job.result()