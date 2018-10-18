#!/usr/bin/env python3
from subscripts.config import executor_labels
from parsl.app.app import python_app

@python_app(executors=executor_labels)
def s2b_freesurfer(sdir, use_gpu, num_cores, stdout, checksum):
    from subscripts.utilities import run,smart_mkdir,smart_remove,write,write_start,write_finish,write_checkpoint
    from subscripts.maskseeds import maskseeds,saveallvoxels
    from os import environ
    from os.path import exists,join,split,splitext
    from shutil import copyfile
    from glob import glob
    write_start(stdout, "s2b_freesurfer")
    T1 = join(sdir,"T1.nii.gz")
    mri_out = join(sdir,"mri","orig","001.mgz")
    subject = split(sdir)[1]
    environ['SUBJECTS_DIR'] = split(sdir)[0]
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
    smart_mkdir(join(sdir,"mri"))
    smart_mkdir(join(sdir,"mri","orig"))
    smart_mkdir(cort_label_dir)
    smart_mkdir(cort_vol_dir)
    smart_mkdir(subcort_vol_dir)
    smart_mkdir(cort_vol_dir_out)
    smart_mkdir(subcort_vol_dir_out)
    smart_mkdir(EDI)
    smart_mkdir(EDI_allvols)
    run("mri_convert {} {}".format(T1,mri_out), stdout)

    if use_gpu:
        if environ and 'CUDA_5_LIB_DIR' not in environ:
            write(stdout, "Error: Environment variable CUDA_5_LIB_DIR not set. Please install CUDA 5 to use Freesurfer GPU functions.")
            return
        environ['CUDA_LIB_DIR'] = environ['CUDA_5_LIB_DIR']
        environ['LD_LIBRARY_PATH'] = "{}:{}".format(environ['CUDA_LIB_DIR'],environ['LD_LIBRARY_PATH'])
        write(stdout, "Running Freesurfer with GPU and {} cores".format(num_cores))
        run("recon-all -s {} -all -no-isrunning -use-gpu -parallel -openmp {}".format(subject, num_cores), stdout)
    elif num_cores > 1:
        write(stdout, "Running Freesurfer with {} cores".format(num_cores))
        run("recon-all -s {} -all -no-isrunning -parallel -openmp {}".format(subject, num_cores), stdout)
    else:
        write(stdout, "Running Freesurfer with a single core")
        run("recon-all -s {} -all -no-isrunning".format(subject), stdout)
    
    run("mri_convert {} {} ".format(join(sdir,"mri","brain.mgz"),T1), stdout)
    run("flirt -in {} -ref {} -omat {}".format(FA,T1,FA2T1), stdout)
    run("convert_xfm -omat {} -inverse {}".format(T12FA,FA2T1), stdout)
    run("mri_annotation2label --subject {} --hemi rh --annotation aparc --outdir {}".format(subject, cort_label_dir), stdout)
    run("mri_annotation2label --subject {} --hemi lh --annotation aparc --outdir {}".format(subject, cort_label_dir), stdout)

    for label in glob(join(cort_label_dir,"*.label")):
        vol_file = join(cort_vol_dir, splitext(split(label)[1])[0] + ".nii.gz")
        run("mri_label2vol --label {} --temp {} --identity --o {}".format(label,T1,vol_file), stdout)

    run("mri_convert {} {}".format(join(sdir,"mri","aseg.mgz"),aseg), stdout)
    for line in open(subcortical_index,"r").readlines():
        num = line.split(":")[0].lstrip().rstrip()
        area = line.split(":")[1].lstrip().rstrip()
        area_out = join(subcort_vol_dir,area + ".nii.gz")
        write(stdout, "Processing " + area + ".nii.gz")
        run("fslmaths {} -uthr {} -thr {} -bin {}".format(aseg,num,num,area_out), stdout)

    for volume in glob(join(cort_vol_dir,"*.nii.gz")):
        out_vol = join(cort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        write(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), stdout)
        run("fslmaths {} -thr 0.2 -bin {} ".format(out_vol,out_vol), stdout)

    for volume in glob(join(subcort_vol_dir,"*.nii.gz")):
        out_vol = join(subcort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        write(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), stdout)
        run("fslmaths {} -thr 0.2 -bin {}".format(out_vol,out_vol), stdout)

    run("fslmaths {} -mul 0 {}".format(FA,bs), stdout)  # For now we fake a bs.nii.gz file
    maskseeds(sdir,join(cort_vol_dir + "_s2fa"),join(cort_vol_dir + "_s2fa_m"),0.05,1,1)
    maskseeds(sdir,join(subcort_vol_dir + "_s2fa"),join(subcort_vol_dir + "_s2fa_m"),0.05,0.4,0.4)
    saveallvoxels(sdir,join(cort_vol_dir + "_s2fa_m"),join(subcort_vol_dir + "_s2fa_m"),allvoxelscortsubcort)
    smart_remove(terminationmask)
    run("fslmaths {} -uthr .15 {}".format(FA, terminationmask), stdout)
    run("fslmaths {} -add {} {}".format(terminationmask, bs, terminationmask), stdout)
    run("fslmaths {} -bin {}".format(terminationmask, terminationmask), stdout)
    run("fslmaths {} -mul {} {}".format(terminationmask, allvoxelscortsubcort, intersection), stdout)
    run("fslmaths {} -sub {} {}".format(terminationmask, intersection, terminationmask), stdout)
    run("fslmaths {} -add {} -add {} {}".format(bs,
                                                join(subcort_vol_dir + "_s2fa_m","lh_thalamus_s2fa.nii.gz"),
                                                join(subcort_vol_dir + "_s2fa_m","rh_thalamus_s2fa.nii.gz"),
                                                exclusion_bsplusthalami), stdout)
    for file in glob(join(sdir,"volumes_cortical_s2fa","*")):
        copyfile(file,EDI_allvols)
    for file in glob(join(sdir,"volumes_subcortical_s2fa","*")):
        copyfile(file,EDI_allvols)
    # copyfile(bs,EDI)
    # copyfile(terminationmask,EDI)
    # copyfile(exclusion_bsplusthalami,EDI)
    # copyfile(allvoxelscortsubcort,EDI)
    write_finish(stdout, "s2b_freesurfer")
    write_checkpoint(sdir, "s2b", checksum)

def create_job(sdir, use_gpu, num_cores, stdout, checksum):
    return s2b_freesurfer(sdir, use_gpu, num_cores, stdout, checksum)
