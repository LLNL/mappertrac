#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint,shutil
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

def run_probtrackx(params):

    pbtx_edges = get_edges_from_file(join(params['script_dir'], 'data/lists/list_edges_tiny.txt'))
    n = 4
    edge_chunks = [pbtx_edges[i * n:(i + 1) * n] for i in range((len(pbtx_edges) + n - 1) // n )]

    start_future = start(params)
    process_futures = []
    for edge_chunk in edge_chunks:
        process_futures.append(process(params, edge_chunk, inputs=[start_future]))
    return combine(params, inputs=process_futures)

@python_app(executors=['worker'])
def start(params, inputs=[]):

    sdir = params['work_dir']
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
    time_log = join(sdir, 'start_time_s3.txt')
    smart_remove(time_log)
    write(time_log, start_time)

@python_app(executors=['worker'])
def process(params, edges, inputs=[]):

    sdir = params['work_dir']
    stdout = params['stdout']

    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bs = join(sdir,"bs.nii.gz")

    # assert exists(bedpostxResults), "Could not find {}".format(bedpostxResults)

@python_app(executors=['worker'])
def combine(params, inputs=[]):

    sdir = params['work_dir']
    stdout = params['stdout']
    
    update_permissions(sdir, params)
    write(join(sdir, 'S3_COMPLETE'))
    
    time_log = join(sdir, 'start_time_s3.txt')
    with open(time_log) as f:
        start_time = float(f.read())
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
