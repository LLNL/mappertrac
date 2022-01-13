#!/usr/bin/env python3
import os,sys,glob,time,csv,math,pprint,shutil
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

@python_app(executors=['worker'])
def run_mrtrix(params):

    input_dir = params['input_dir']
    sdir = params['work_dir']
    ID = params['ID']
    stdout = params['stdout']

    start_time = time.time()
    start_str = f'''
=====================================
{get_time_date()}
Started step 2b: mrtrix
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

    unzipped_dwi = join(sdir, 'hardi.nii')
    dwi_mif = join(sdir, 'DWI.mif')
    dwi_mif_biascorrect = join(sdir, 'DWI_bias_fsl.mif')
    wm_norm = join(sdir, 'wmfod_norm.mif')
    sdir_tmp = join(sdir, "tmp")

    # Convert freesurfer output to 5 tissue MRTrix format
    run(f'5ttgen freesurfer {sdir}/mri/aparc+aseg.mgz {sdir}/5TT.mif -scratch {sdir_tmp} -force -info', params)

    # Convert NIfTI DWI to MRTrix mif
    run(f'gunzip -c {work_dwi} > {unzipped_dwi}', params)
    run(f'mrconvert {unzipped_dwi} {dwi_mif} -fslgrad {work_bvec} {work_bval} -datatype float32 -stride 0,0,0,1 -force -info', params)

    # Perform bias correction
    run(f'dwibiascorrect fsl {dwi_mif} {dwi_mif_biascorrect} -bias {sdir}/bias_fsl_field.mif -scratch {sdir_tmp} -force -info', params)

    # Extract single-shell response function
    run(f'dwi2response tournier {dwi_mif_biascorrect} {sdir}/response_wm.txt -scratch {sdir_tmp} -force -info', params)
    
    # Generate mask and FODs
    run(f'dwi2mask {dwi_mif_biascorrect} {sdir}/DWI_mask.mif -force -info', params)
    run(f'dwi2fod csd {dwi_mif_biascorrect} {sdir}/response_wm.txt {sdir}/wmfod.mif -mask {sdir}/DWI_mask.mif -force -info', params)

    # Perform normalization
    run(f'mtnormalise {sdir}/wmfod.mif {wm_norm} -mask {sdir}/DWI_mask.mif -force -info -debug', params)

    # Run tractography
    # TODO: pass sample count to select arg
    run(f'''tckgen \
        -info \
        -force \
        -algorithm iFOD2 \
        -select 5000 \
        -act {sdir}/5TT.mif -backtrack -crop_at_gmwmi \
        -max_attempts_per_seed 1000 \
        -seed_dynamic {wm_norm} \
        -output_seeds {sdir}/seeds.txt \
        {wm_norm} {sdir}/tracks.tck |& tee {sdir}/tckgen_log.txt
        ''', params)
    run(f'tckmap {sdir}/tracks.tck {sdir}/tracks.img.mif -template {wm_norm} -force -info', params)

    run(f'labelconvert {sdir}/aparc+aseg.mgz /opt/freesurfer/FreeSurferColorLUT.txt ' +
        f'/opt/mrtrix3-994498557037c9e4f7ba67f255820ef84ea899d9/share/mrtrix3/labelconvert/fs_default.txt ' + 
        f'{sdir}/nodes.mif -force -info', params)
    run(f'tck2connectome {sdir}/tracks.tck {sdir}/nodes.mif {sdir}/mrtrix_connectome.csv -force -info', params)
    
    # Remove large redundant files
    smart_remove(dwi_mif)
    smart_remove(dwi_mif_biascorrect)
    smart_remove(wm_norm)
    smart_remove(unzipped_dwi)

    update_permissions(sdir, params)

    write(join(sdir, 'S2B_COMPLETE'))
    
    finish_str = f'''
=====================================
{get_time_date()}
Finished step 2b: mrtrix
Arguments: 
{pprint.pformat(params, width=1)}
Total time: {get_time_string(time.time() - start_time)} (HH:MM:SS)
=====================================
'''
    write(stdout, finish_str)
    print(finish_str)
