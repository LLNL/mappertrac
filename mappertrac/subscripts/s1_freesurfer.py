#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

@python_app(executors=['worker'])
def run_freesurfer(params):

    input_dir = params['input_dir']
    output_dir = params['output_dir']
    subject_dir = params['work_dir']
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
    assert exists(input_dwi), f'Missing file {input_dwi}'
    work_dwi = join(subject_dir, 'hardi.nii.gz')
    smart_copy(input_dwi, work_dwi)
    
    eddy_prefix = join(subject_dir, 'data_eddy')
    bet = join(subject_dir, 'data_bet.nii.gz')

    for _ in glob(f'{eddy_prefix}_tmp????.*') + glob(f'{eddy_prefix}_ref*'):
        smart_remove(_)
    run(f'fslroi {work_dwi} {eddy_prefix}_ref 0 1', params)
    run(f'fslsplit {work_dwi} {eddy_prefix}_tmp', params)

    timeslices = glob(f'{eddy_prefix}_tmp????.*')
    for _ in timeslices:
        run(f'flirt -in {_} -ref {eddy_prefix}_ref -nosearch -interp trilinear -o {_} -paddingsize 1', params)
    run(f'fslmerge -t {eddy_prefix}.nii.gz {" ".join(timeslices)}', params)
    run(f'bet {eddy_prefix}.nii.gz {bet} -m -f 0.3', params)

    # Save NIfTI files in BIDS rawdata directory
    rawdata_dir = subject_dir.replace('/derivatives/', '/rawdata/')
    smart_copy(input_dir, rawdata_dir)

    write(join(subject_dir, 'S1_COMPLETE'))
    
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
