#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint,shutil
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

@python_app(executors=['worker'])
def run_probtrackx(params):

    input_dir = params['input_dir']
    output_dir = params['output_dir']
    sdir = params['work_dir']
    ID = params['ID']
    stdout = params['stdout']

    start_time = time.time()
    start_str = f'''
=====================================
{get_time_date()}
Started step 3: probtrackx
Arguments: 
{pprint.pformat(params, width=1)}
=====================================
'''
    write(stdout, start_str)
    print(start_str)


    
    update_permissions(sdir, params)
    write(join(sdir, 'S3_COMPLETE'))
    
    finish_str = f'''
=====================================
{get_time_date()}
Finished step 3: probtrackx
Arguments: 
{pprint.pformat(params, width=1)}
Total time: {get_time_string(time.time() - start_time)} (HH:MM:SS)
=====================================
'''
    write(stdout, finish_str)
    print(finish_str)
