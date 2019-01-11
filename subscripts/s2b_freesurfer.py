#!/usr/bin/env python3
from parsl.app.app import python_app

@python_app(executors=['s2b'], cache=True)
def s2b_1_recon_all(params, inputs=[]):
    import time
    from subscripts.utilities import run,smart_mkdir,smart_remove,write,record_apptime,record_start,copy_dir
    from os import environ
    from os.path import exists,join,split,basename
    sdir = params['sdir']
    work_sdir = params['work_sdir']
    if work_sdir:
        smart_mkdir(work_sdir)
        sdir = work_sdir
    stdout = params['stdout']
    container = params['container']
    cores_per_task = params['cores_per_task']
    use_gpu = params['use_gpu']
    group = params['group']
    subject = split(sdir)[1]
    record_start(params)
    start_time = time.time()
    T1 = join(sdir,"T1.nii.gz")
    mri_out = join(sdir,"mri","orig","001.mgz")
    smart_mkdir(join(sdir,"mri"))
    smart_mkdir(join(sdir,"mri","orig"))
    run("mri_convert {} {}".format(T1,mri_out), params)

    if not container:
        environ['SUBJECTS_DIR'] = split(sdir)[0]

    if use_gpu:
        write(stdout, "Running Freesurfer with GPU and {} cores".format(cores_per_task))
        freesurfer_sh = join(sdir, "freesurfer.sh")
        write(freesurfer_sh, "export CUDA_LIB_DIR=$CUDA_5_LIB_DIR\n" +
                  "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:$LD_LIBRARY_PATH\n" +
                  "recon-all -s {} -all -no-isrunning -use-gpu -parallel -openmp {}".format(subject, cores_per_task))
        run("sh " + freesurfer_sh, params)
    elif cores_per_task > 1:
        write(stdout, "Running Freesurfer with {} cores".format(cores_per_task))
        run("recon-all -s {} -all -no-isrunning -parallel -openmp {}".format(subject, cores_per_task), params)
    else:
        write(stdout, "Running Freesurfer with a single core")
        run("recon-all -s {} -all -no-isrunning".format(subject), params)
    if work_sdir:
        copy_dir(work_sdir, params['sdir'])
        run("rm -Rf {}".format(work_sdir))
    record_apptime(params, start_time, 1)

@python_app(executors=['s2b'], cache=True)
def s2b_2_process_vols(params, inputs=[]):
    import time
    from subscripts.utilities import run,smart_mkdir,smart_remove,write,record_apptime,record_finish,update_permissions
    from subscripts.maskseeds import maskseeds,saveallvoxels
    from os.path import exists,join,split,splitext
    from os import environ
    from shutil import copy
    from glob import glob
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    cores_per_task = params['cores_per_task']
    group = params['group']
    start_time = time.time()
    T1 = join(sdir,"T1.nii.gz")
    subject = split(sdir)[1]
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
    smart_mkdir(cort_label_dir)
    smart_mkdir(cort_vol_dir)
    smart_mkdir(subcort_vol_dir)
    smart_mkdir(cort_vol_dir_out)
    smart_mkdir(subcort_vol_dir_out)
    smart_mkdir(EDI)
    smart_mkdir(EDI_allvols)

    if not container:
        environ['SUBJECTS_DIR'] = split(sdir)[0]
        
    run("mri_convert {} {} ".format(join(sdir,"mri","brain.mgz"),T1), params)
    run("flirt -in {} -ref {} -omat {}".format(FA,T1,FA2T1), params)
    run("convert_xfm -omat {} -inverse {}".format(T12FA,FA2T1), params)
    run("mri_annotation2label --subject {} --hemi rh --annotation aparc --outdir {}".format(subject, cort_label_dir), params)
    run("mri_annotation2label --subject {} --hemi lh --annotation aparc --outdir {}".format(subject, cort_label_dir), params)

    for label in glob(join(cort_label_dir,"*.label")):
        vol_file = join(cort_vol_dir, splitext(split(label)[1])[0] + ".nii.gz")
        run("mri_label2vol --label {} --temp {} --identity --o {}".format(label,T1,vol_file), params)

    run("mri_convert {} {}".format(join(sdir,"mri","aseg.mgz"),aseg), params)
    for line in open(subcortical_index,"r").readlines():
        num = line.split(":")[0].lstrip().rstrip()
        area = line.split(":")[1].lstrip().rstrip()
        area_out = join(subcort_vol_dir,area + ".nii.gz")
        write(stdout, "Processing " + area + ".nii.gz")
        run("fslmaths {} -uthr {} -thr {} -bin {}".format(aseg,num,num,area_out), params)

    for volume in glob(join(cort_vol_dir,"*.nii.gz")):
        out_vol = join(cort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        write(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), params)
        run("fslmaths {} -thr 0.2 -bin {} ".format(out_vol,out_vol), params)

    for volume in glob(join(subcort_vol_dir,"*.nii.gz")):
        out_vol = join(subcort_vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_s2fa.nii.gz")
        write(stdout, "Processing {} -> {}".format(split(volume)[1], split(out_vol)[1]))
        run("flirt -in {} -ref {} -out {}  -applyxfm -init {}".format(volume,FA,out_vol,T12FA), params)
        run("fslmaths {} -thr 0.2 -bin {}".format(out_vol,out_vol), params)

    run("fslmaths {} -mul 0 {}".format(FA,bs), params)  # For now we fake a bs.nii.gz file
    maskseeds(sdir,join(cort_vol_dir + "_s2fa"),join(cort_vol_dir + "_s2fa_m"),0.05,1,1,params)
    maskseeds(sdir,join(subcort_vol_dir + "_s2fa"),join(subcort_vol_dir + "_s2fa_m"),0.05,0.4,0.4,params)
    saveallvoxels(sdir,join(cort_vol_dir + "_s2fa_m"),join(subcort_vol_dir + "_s2fa_m"),allvoxelscortsubcort,params)
    smart_remove(terminationmask)
    run("fslmaths {} -uthr .15 {}".format(FA, terminationmask), params)
    run("fslmaths {} -add {} {}".format(terminationmask, bs, terminationmask), params)
    run("fslmaths {} -bin {}".format(terminationmask, terminationmask), params)
    run("fslmaths {} -mul {} {}".format(terminationmask, allvoxelscortsubcort, intersection), params)
    run("fslmaths {} -sub {} {}".format(terminationmask, intersection, terminationmask), params)
    run("fslmaths {} -add {} -add {} {}".format(bs,
                                                join(subcort_vol_dir + "_s2fa_m","lh_thalamus_s2fa.nii.gz"),
                                                join(subcort_vol_dir + "_s2fa_m","rh_thalamus_s2fa.nii.gz"),
                                                exclusion_bsplusthalami), params)
    for file in glob(join(sdir,"volumes_cortical_s2fa","*.nii.gz")):
        copy(file,EDI_allvols)
    for file in glob(join(sdir,"volumes_subcortical_s2fa","*.nii.gz")):
        copy(file,EDI_allvols)
    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def run_s2b(params, inputs):
    s2b_1_future = s2b_1_recon_all(params, inputs=inputs)
    return s2b_2_process_vols(params, inputs=[s2b_1_future])
