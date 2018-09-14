#!/usr/bin/env python3
import argparse
import sys    
import os
import multiprocessing
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *
from glob import glob
from maskseeds import *
from posix import remove
from utilities import *

cortical_dir = "label_cortical"
vol_dir = "volumes_cortical"
sub_vol_dir = "volumes_subcortical"
threshold = "0.2"

parser = argparse.ArgumentParser(description='Preprocess Freesurfer data')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
parser.add_argument('--num_cores', help='Number of cores for OpenMP', default=multiprocessing.cpu_count())
args = parser.parse_args()

start_time = printStart()

odir = abspath(args.output_dir)
T1 = join(odir,"T1.nii.gz")
subject = split(odir)[1]

# cores = max(int(int(args.num_cores) / 2), 1) # use half of actual, due to left-right hemisphere parallelization
# environ["OMP_NUM_THREADS"] = str(cores)

environ['SUBJECTS_DIR'] = split(odir)[0]

# Make the output directories if necessary
if not exists(join(odir,"mri")):
    mkdir(join(odir,"mri"))

if not exists(join(odir,"mri/orig")):
    mkdir(join(odir,"mri/orig"))

if args.force or not exists(join(odir,"mri/orig/001.mgz")):
    run("mri_convert {} {}".format(T1,join(odir,"mri/orig/001.mgz")))

if args.force or not exists(join(odir,"mri","aparc+aseg.mgz")):
    if "OMP_NUM_THREADS" in environ and int(environ["OMP_NUM_THREADS"]) > 2:
        run("recon-all -s {} -parallel -openmp {} -all -no-isrunning".format(subject, environ["OMP_NUM_THREADS"]))
    else:
        run("recon-all -s {} -all -no-isrunning".format(subject))

run("mri_convert {} {} ".format(join(odir,"mri","brain.mgz"),T1))

if args.force or not exists(join(odir,"FA2T1.mat")):
    run("flirt -in {} -ref {} -omat {}".format(join(odir,"FA.nii.gz"),T1,join(odir,"FA2T1.mat")))

if args.force or not exists(join(odir,"T12FA.mat")):
    run("convert_xfm -omat {} -inverse {}".format(join(odir,"T12FA.mat"),join(odir,"FA2T1.mat")))

if not exists(join(odir,"label_cortical")):
    mkdir(join(odir,"label_cortical"))

# extract cortical labels (extralabels)
if args.force or not exists(join(odir,cortical_dir,"rh.temporalpole.label")):
    run("mri_annotation2label --subject {} --hemi rh --annotation aparc --outdir {}".format(subject,join(odir,cortical_dir)))

if args.force or not exists(join(odir,cortical_dir,"lh.temporalpole.label")):
    run("mri_annotation2label --subject {} --hemi lh --annotation aparc --outdir {}".format(subject,join(odir,cortical_dir)))

if not exists(join(odir,vol_dir)):
    mkdir(join(odir,vol_dir))

# extract volume labels (label2vol)
for label in glob(join(odir,cortical_dir,"*.label")):
    vol_name = splitext(split(label)[1])[0] + ".nii.gz"

    if args.force or not exists(join(odir,vol_dir,vol_name)):
        run("mri_label2vol --label {} --temp {} --identity --o {}".format(label,T1,join(odir,vol_dir,vol_name)))

# make_subcortical_vols
if args.force or not exists(join(odir,"aseg.nii.gz")):
    run("mri_convert {} {}".format(join(odir,"mri","aseg.mgz"),join(odir,"aseg.nii.gz")))

if not exists(join(odir,sub_vol_dir)):
    mkdir(join(odir,sub_vol_dir))

if args.force or not exists(join(odir,sub_vol_dir,"lh_acumbens.nii.gz")):
    # indices = join(split(abspath(sys.argv[0]))[0],"subcortical_index")
    indices = "subcortical_index.txt"

    for line in open(indices,"r").readlines():
        num = line.split(":")[0].lstrip().rstrip()
        area = line.split(":")[1].lstrip().rstrip()
        print("Processing " + area + ".nii.gz")
        run("fslmaths {} -uthr {} -thr {} -bin {}".format(join(odir,"aseg.nii.gz"),num,num,
                                                                            join(odir,sub_vol_dir,area + ".nii.gz")))

vol_dir_out = vol_dir + "_s2fa"
if not exists(join(odir,vol_dir_out)):
    mkdir(join(odir,vol_dir_out))
if args.force or not exists(join(odir,vol_dir_out,"rh.bankssts_s2fa.nii.gz")):
    for volume in glob(join(odir,vol_dir,"*.nii.gz")):
        name = splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz"
        out_vol = join(odir,vol_dir_out,name)
        print("Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,join(odir,"FA.nii.gz"),
                                                                                    out_vol,join(odir,"T12FA.mat")))
        run("fslmaths {} -thr {} -bin {} ".format(out_vol,threshold,out_vol))


vol_dir_out = sub_vol_dir + "_s2fa"
if not exists(join(odir,vol_dir_out)):
    mkdir(join(odir,vol_dir_out))
if args.force or not exists(join(odir,vol_dir_out,"lh_acumbens_s2fa.nii.gz")):
    for volume in glob(join(odir,sub_vol_dir,"*.nii.gz")):
        out_vol = join(odir,vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        print("Processing ",split(volume)[1]," -> ",split(out_vol)[1])
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,join(odir,"FA.nii.gz"),
                                                                                        out_vol,join(odir,"T12FA.mat")))
        run("fslmaths {} -thr {} -bin {} ".format(out_vol,threshold,out_vol))

############
# FOr now we fake a bs.nii.gz file
if not exists(join(odir,"bs.nii.gz")):
    run("fslmaths {} -mul 0 {}".format(join(odir,"FA.nii.gz"),join(odir,"bs.nii.gz")))

############
# maskseeds
if exists(join(odir,"bs.nii.gz")):
    maskseeds(odir,join(odir,vol_dir + "_s2fa"),join(odir,vol_dir + "_s2fa_m"),0.05,1,1)
    maskseeds(odir,join(odir,sub_vol_dir + "_s2fa"),join(odir,sub_vol_dir + "_s2fa_m"),0.05,0.4,0.4)

    saveallvoxels(odir,join(odir,vol_dir + "_s2fa_m"),join(odir,sub_vol_dir + "_s2fa_m"),join(odir,"allvoxelscortsubcort.nii.gz"),args.force)

    if exists(join(odir,"terminationmask.nii.gz")):
        remove(join(odir,"terminationmask.nii.gz"))

    run("fslmaths {} -uthr .15 {}".format(join(odir,"FA.nii.gz"),join(odir,"terminationmask.nii.gz")))

    run("fslmaths {} -add {} {}".format(join(odir,"terminationmask.nii.gz"),join(odir,"bs.nii.gz"),join(odir,"terminationmask.nii.gz")))
    run("fslmaths {} -bin {}".format(join(odir,"terminationmask.nii.gz"),join(odir,"terminationmask.nii.gz")))
    run("fslmaths {} -mul {} {}".format(join(odir,"terminationmask.nii.gz"),join(odir,"allvoxelscortsubcort.nii.gz"),join(odir,"intersection.nii.gz")))
    run("fslmaths {} -sub {} {}".format(join(odir,"terminationmask.nii.gz"),join(odir,"intersection.nii.gz"),join(odir,"terminationmask.nii.gz")))

    run("fslmaths {} -add {} -add {} {}".format(join(odir,"bs.nii.gz"),
                                                join(odir,sub_vol_dir + "_s2fa_m","lh_thalamus_s2fa.nii.gz"),
                                                join(odir,sub_vol_dir + "_s2fa_m","rh_thalamus_s2fa.nii.gz"),
                                                join(odir,"exlusion_bsplusthalami.nii.gz")))
############
############
############



# exit(0)

if not exists(join(odir,"EDI")):
    mkdir(join(odir,"EDI"))

if not exists(join(odir,"EDI","allvols")):
    mkdir(join(odir,"EDI","allvols"))

if args.force or not exists(join(odir,"EDI","allvols","rh_thalamus_s2fa.nii.gz")):
    for files in glob(join(odir,"volumes_cortical_s2fa","*")):
        copy(files,join(odir,"EDI","allvols"))

    for files in glob(join(odir,"volumes_subcortical_s2fa","*")):
        copy(files,join(odir,"EDI","allvols"))

if exists(join(odir,"bs.nii.gz")):
    copy(join(odir,"bs.nii.gz"),join(odir,"EDI"))
    copy(join(odir,"terminationmask.nii.gz"),join(odir,"EDI"))
    copy(join(odir,"exlusion_bsplusthalami.nii.gz"),join(odir,"EDI"))
    copy(join(odir,"allvoxelscortsubcort.nii.gz"),join(odir,"EDI"))
else:
    copy(join(odir,"terminationmask.nii.gz"),join(odir,"EDI"))

if args.output_time:
    printFinish(start_time)
    