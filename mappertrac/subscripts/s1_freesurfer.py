#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint,shutil
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

@python_app(executors=['worker'])
def run_freesurfer(params):

    input_dir = params['input_dir']
    sdir = params['work_dir']
    ID = params['ID']
    stdout = params['stdout']

    start_time = time.time()
    start_str = f'''
=====================================
{get_time_date()}
Started step 1: freesurfer
Arguments: 
{pprint.pformat(params, width=1)}
=====================================
'''
    write(stdout, start_str)
    print(start_str)

    input_dwi = join(input_dir, 'dwi', f'{ID}_dwi.nii.gz')
    input_bval = join(input_dir, 'dwi', f'{ID}_dwi.bval')
    input_bvec = join(input_dir, 'dwi', f'{ID}_dwi.bvec')
    input_T1 = join(input_dir, 'anat', f'{ID}_T1w.nii.gz')
    for _ in [input_dwi, input_bval, input_bvec, input_T1]:
        assert exists(_), f'Missing file {_}'
    
    smart_mkdir(sdir)
    work_dwi = join(sdir, 'hardi.nii.gz')
    work_bval = join(sdir, 'bvals')
    work_bvec = join(sdir, 'bvecs')
    work_T1 = join(sdir, 'T1.nii.gz')
    smart_copy(input_dwi, work_dwi)
    smart_copy(input_bval, work_bval)
    smart_copy(input_bvec, work_bvec)
    smart_copy(input_T1, work_T1)

    # Save NIfTI files in BIDS rawdata directory
    rawdata_dir = sdir.replace('/derivatives/', '/rawdata/')
    smart_copy(input_dir, rawdata_dir)

    ##################################
    # dti-preproc
    ##################################

    eddy_prefix = join(sdir, 'data_eddy')
    data_eddy = f'{eddy_prefix}.nii.gz'
    bet = join(sdir, 'data_bet.nii.gz')

    for _ in glob(f'{eddy_prefix}_tmp????.*') + glob(f'{eddy_prefix}_ref*'):
        smart_remove(_)

    run(f'fslroi {work_dwi} {eddy_prefix}_ref 0 1', params)
    run(f'fslsplit {work_dwi} {eddy_prefix}_tmp', params)

    timeslices = glob(f'{eddy_prefix}_tmp????.*')
    for _ in timeslices:
        run(f'flirt -in {_} -ref {eddy_prefix}_ref -nosearch -interp trilinear -o {_} -paddingsize 1', params)
    run(f'fslmerge -t {data_eddy} {" ".join(timeslices)}', params)
    run(f'bet {data_eddy} {bet} -m -f 0.3', params)

    ##################################
    # recon-all
    ##################################

    bet_mask = join(sdir, 'data_bet_mask.nii.gz')
    dti_params = join(sdir, 'DTIparams')
    dti_L1 = f'{dti_params}_L1.nii.gz'
    dti_L2 = f'{dti_params}_L2.nii.gz'
    dti_L3 = f'{dti_params}_L3.nii.gz'
    dti_MD = f'{dti_params}_MD.nii.gz'
    dti_RD = f'{dti_params}_RD.nii.gz'
    dti_MD = f'{dti_params}_MD.nii.gz'
    dti_AD = f'{dti_params}_AD.nii.gz'
    dti_FA = f'{dti_params}_FA.nii.gz'
    FA = join(sdir, 'FA.nii.gz')
    if exists(bet_mask):
        run(f'dtifit --verbose -k {data_eddy} -o {dti_params} -m {bet_mask} -r {work_bvec} -b {work_bval}', params)
        run(f'fslmaths {dti_L1} -add {dti_L2} -add {dti_L3} -div 3 {dti_MD}', params)
        run(f'fslmaths {dti_L2} -add {dti_L3} -div 2 {dti_RD}', params)
        smart_copy(dti_L1, dti_AD)
        smart_copy(dti_FA, FA)
    else:
        write(stdout, "Warning: failed to generate masked outputs")
        raise Exception(f"Failed BET step. Please check {stdout} for more info.")

    for _ in glob(f'{eddy_prefix}_tmp????.*') + glob(f'{eddy_prefix}_ref*'):
        smart_remove(_)

    # fsdir = join(sdir, 'freesurfer')
    # smart_mkdir(fsdir)
    # for _ in [work_dwi, work_bval, work_bvec, work_T1, bet_mask, dti_L1, dti_L2, dti_L3, dti_MD, dti_RD, dti_MD, dti_AD, dti_FA, FA]:
    #     smart_copy(_, join(fsdir, basename(_)))

    mri_out = join(sdir, 'mri', 'orig', '001.mgz')
    smart_mkdir(join(sdir, 'mri', 'orig'))
    run(f'mri_convert {work_T1} {mri_out}', params)

    ncores = int(multiprocessing.cpu_count())
    write(stdout, f'Running Freesurfer with {ncores} cores')
    run(f'recon-all -s . -all -notal-check -no-isrunning -parallel -openmp {ncores}', params)

    ##################################
    # mri_annotation2label
    ##################################
    mri_brain = join(sdir, 'mri', 'brain.mgz')
    mri_aseg = join(sdir, 'mri', 'aseg.mgz')
    aseg = join(sdir, 'aseg.nii.gz')
    bs = join(sdir, 'bs.nii.gz')
    FA2T1 = join(sdir, 'FA2T1.mat')
    T12FA = join(sdir, 'T12FA.mat')
    cort_label_dir = join(sdir, 'label_cortical')
    cort_vol_dir = join(sdir, 'volumes_cortical')
    cort_vol_dir_out = cort_vol_dir + '_s2fa'
    subcort_vol_dir = join(sdir, 'volumes_subcortical')
    subcort_vol_dir_out = subcort_vol_dir + '_s2fa'
    terminationmask = join(sdir, 'terminationmask.nii.gz')
    allvoxelscortsubcort = join(sdir, 'allvoxelscortsubcort.nii.gz')
    intersection = join(sdir, 'intersection.nii.gz')
    subcortical_index = [
        '10:lh_thalamus', '11:lh_caudate', '12:lh_putamen', '13:lh_pallidum', '17:lh_hippocampus', '18:lh_amygdala', '26:lh_acumbens', 
        '49:rh_thalamus', '50:rh_caudate', '51:rh_putamen', '52:rh_pallidum', '53:rh_hippocampus', '54:rh_amygdala', '58:rh_acumbens',
    ]
    EDI = join(sdir, 'EDI')
    EDI_allvols = join(EDI, 'allvols')

    smart_mkdir(cort_label_dir)
    smart_mkdir(cort_vol_dir)
    smart_mkdir(subcort_vol_dir)
    smart_mkdir(cort_vol_dir_out)
    smart_mkdir(subcort_vol_dir_out)
    smart_mkdir(EDI)
    smart_mkdir(EDI_allvols)

    run(f'mri_convert {mri_brain} {work_T1} ', params)
    run(f'mri_convert {mri_aseg} {aseg}', params)
    run(f'flirt -in {FA} -ref {work_T1} -omat {FA2T1}', params)
    run(f'convert_xfm -omat {T12FA} -inverse {FA2T1}', params)
    run(f'mri_annotation2label --subject . --hemi rh --annotation aparc --outdir {cort_label_dir}', params)
    run(f'mri_annotation2label --subject . --hemi lh --annotation aparc --outdir {cort_label_dir}', params)

    for label in glob(join(cort_label_dir, '*.label')):
        vol_file = join(cort_vol_dir, splitext(split(label)[1])[0] + '.nii.gz')
        run(f'mri_label2vol --label {label} --temp {work_T1} --identity --o {vol_file}', params)

    for line in subcortical_index:
        num = line.split(':')[0].lstrip().rstrip()
        area = line.split(':')[1].lstrip().rstrip()
        area_out = join(subcort_vol_dir, area + '.nii.gz')
        write(stdout, f'Processing {area}.nii.gz')
        run(f'fslmaths {aseg} -uthr {num} -thr {num} -bin {area_out}', params)

    for volume in glob(join(cort_vol_dir, '*.nii.gz')):
        out_vol = join(cort_vol_dir_out, splitext(splitext(split(volume)[1])[0])[0] + '_s2fa.nii.gz')
        write(stdout, f'Processing {split(volume)[1]} -> {split(out_vol)[1]}')
        run(f'flirt -in {volume} -ref {FA} -out {out_vol}  -applyxfm -init {T12FA}', params)
        run(f'fslmaths {out_vol} -thr 0.2 -bin {out_vol} ', params)

    for volume in glob(join(subcort_vol_dir, '*.nii.gz')):
        out_vol = join(subcort_vol_dir_out, splitext(splitext(split(volume)[1])[0])[0] + '_s2fa.nii.gz')
        write(stdout, f'Processing {split(volume)[1]} -> {split(out_vol)[1]}')
        run(f'flirt -in {volume} -ref {FA} -out {out_vol}  -applyxfm -init {T12FA}', params)
        run(f'fslmaths {out_vol} -thr 0.2 -bin {out_vol}', params)

    run(f'fslmaths {FA} -mul 0 {bs}', params)  # For now we fake a bs.nii.gz file
    maskseeds(sdir, join(cort_vol_dir + '_s2fa'), join(cort_vol_dir + '_s2fa_m'), 0.05, 1, 1, params)
    maskseeds(sdir, join(subcort_vol_dir + '_s2fa'), join(subcort_vol_dir + '_s2fa_m'), 0.05, 0.4, 0.4, params)
    saveallvoxels(sdir, join(cort_vol_dir + '_s2fa_m'), join(subcort_vol_dir + '_s2fa_m'), allvoxelscortsubcort, params)

    ##################################
    # termination mask
    ##################################
    smart_remove(terminationmask)
    run(f'fslmaths {FA} -uthr .15 {terminationmask}'.format(FA, terminationmask), params)
    run(f'fslmaths {terminationmask} -add {bs} {terminationmask}', params)
    run(f'fslmaths {terminationmask} -bin {terminationmask}', params)
    run(f'fslmaths {terminationmask} -mul {allvoxelscortsubcort} {intersection}', params)
    run(f'fslmaths {terminationmask} -sub {intersection} {terminationmask}', params)
    for file in glob(join(sdir, 'volumes_cortical_s2fa','*.nii.gz')):
        shutil.copy(file, EDI_allvols)
    for file in glob(join(sdir, 'volumes_subcortical_s2fa','*.nii.gz')):
        shutil.copy(file, EDI_allvols)
    validate(terminationmask, params)
    update_permissions(sdir, params)

    write(join(sdir, 'S1_COMPLETE'))
    
    finish_str = f'''
=====================================
{get_time_date()}
Finished step 1: freesurfer
Arguments: 
{pprint.pformat(params, width=1)}
Total time: {get_time_string(time.time() - start_time)} (HH:MM:SS)
=====================================
'''
    write(stdout, finish_str)
    print(finish_str)
