#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint,shutil
import GPUtil
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

@python_app(executors=['worker'])
def run_bedpostx(params):

    input_dir = params['input_dir']
    sdir = params['work_dir']
    ID = params['ID']
    stdout = params['stdout']

    assert exists(join(sdir, 'S1_COMPLETE')), 'Subject {sdir} must first run --freesurfer'
    assert len(GPUtil.getAvailable(includeNan=True)) > 0, 'Failed to find CUDA-capable GPU'

    start_time = time.time()
    start_str = f'''
=====================================
{get_time_date()}
Started step 2: bedpostx
Arguments: 
{pprint.pformat(params, width=1)}
=====================================
'''
    write(stdout, start_str)
    print(start_str)

    bedpostx = join(sdir,'bedpostx_b1000')
    bedpostxResults = join(sdir,'bedpostx_b1000.bedpostX')
    th1 = join(bedpostxResults, 'merged_th1samples')
    ph1 = join(bedpostxResults, 'merged_ph1samples')
    th2 = join(bedpostxResults, 'merged_th2samples')
    ph2 = join(bedpostxResults, 'merged_ph2samples')
    dyads1 = join(bedpostxResults, 'dyads1')
    dyads2 = join(bedpostxResults, 'dyads2')
    brain_mask = join(bedpostxResults, 'nodif_brain_mask')
    smart_remove(bedpostxResults)
    smart_mkdir(bedpostx)
    smart_mkdir(bedpostxResults)
    smart_copy(join(sdir, 'data_eddy.nii.gz'), join(bedpostx, 'data.nii.gz'))
    smart_copy(join(sdir, 'data_bet_mask.nii.gz'), join(bedpostx, 'nodif_brain_mask.nii.gz'))
    smart_copy(join(sdir, 'bvals'), join(bedpostx, 'bvals'))
    smart_copy(join(sdir, 'bvecs'), join(bedpostx, 'bvecs'))

    bedpostx_sh = join(sdir, 'bedpostx.sh')
    smart_remove(bedpostx_sh)
    write(bedpostx_sh, 'export CUDA_LIB_DIR=$CUDA_8_LIB_DIR\n' +
                       'export LD_LIBRARY_PATH=$CUDA_LIB_DIR:$LD_LIBRARY_PATH\n' +
                       'bedpostx_gpu /mnt/bedpostx_b1000 -NJOBS 4')
    gpu_params = params.copy()
    gpu_params['use_gpu'] = True
    run('sh ' + bedpostx_sh, gpu_params)
    
    # hacky validation step
    with open(stdout) as f:
        log_content = f.read()
        for i in range(1, 5):
            assert('{:d} parts processed out of 4'.format(i) in log_content)

    run(f'make_dyadic_vectors {th1} {ph1} {brain_mask} {dyads1}', params)
    run(f'make_dyadic_vectors {th2} {ph2} {brain_mask} {dyads2}', params)
    update_permissions(sdir, params)

    write(join(sdir, 'S2_COMPLETE'))
    
    finish_str = f'''
=====================================
{get_time_date()}
Finished step 2: bedpostx
Arguments: 
{pprint.pformat(params, width=1)}
Total time: {get_time_string(time.time() - start_time)} (HH:MM:SS)
=====================================
'''
    write(stdout, finish_str)
    print(finish_str)
