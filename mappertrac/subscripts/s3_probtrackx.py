#!/usr/bin/env python3
import os,sys,glob,multiprocessing,time,csv,math,pprint,shutil,platform,fcntl,errno,tempfile,json,psutil,random
import scipy.io
import numpy as np
from parsl.app.app import python_app
from os.path import *
from mappertrac.subscripts import *

EDGE_LIST = 'data/lists/list_edges_reduced.txt'

def run_probtrackx(params):

    sdir = params['work_dir']
    assert exists(join(sdir, 'S1_COMPLETE')), 'Subject {sdir} must first run --freesurfer'
    assert exists(join(sdir, 'S2_COMPLETE')), 'Subject {sdir} must first run --bedpostx'

    pbtx_edges = get_edges_from_file(join(params['script_dir'], EDGE_LIST))
    edges_per_chunk = 4
    n = edges_per_chunk
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

    sdir = params['work_dir']
    output_dir = params['output_dir']
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    derivatives_dir_tmp = join(output_dir, 'derivatives', "tmp")
    sdir_tmp = join(sdir, "tmp")
    smart_remove(pbtk_dir)
    smart_remove(connectome_dir)
    smart_remove(sdir_tmp)
    smart_mkdir(pbtk_dir)
    smart_mkdir(connectome_dir)
    smart_mkdir(sdir_tmp)
    time.sleep(random.randrange(0, 10)) # random sleep to avoid parallel collision
    smart_mkdir(derivatives_dir_tmp)

@python_app(executors=['worker'])
def process(params, edges, inputs=[]):

    sdir = params['work_dir']
    stdout = params['stdout']
    output_dir = params['output_dir']
    pbtx_sample_count = params['pbtx_sample_count']
    derivatives_dir_tmp = join(output_dir, 'derivatives', "tmp")
    sdir_tmp = join(sdir, "tmp")
    EDI_allvols = join(sdir,"EDI","allvols")
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bs = join(sdir,"bs.nii.gz")

    ##################################
    # Tractography
    ##################################
    for edge in edges:
        a, b = edge
        a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
        b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
        tmp = join(sdir, "tmp", "{}_to_{}".format(a, b))
        a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
        a_to_b_file = join(pbtk_dir,a_to_b_formatted)
        waypoints = join(tmp,"waypoint.txt")
        waytotal = join(tmp, "waytotal")
        assert exists(a_file) and exists(b_file), "Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file)
        smart_remove(a_to_b_file)
        smart_remove(tmp)
        smart_mkdir(tmp)
        write(stdout, "Running subproc: {} to {}".format(a, b))
        write(waypoints, b_file.replace(sdir, "/mappertrac"))

        exclusion = join(tmp,"exclusion.nii.gz")
        termination = join(tmp,"termination.nii.gz")
        run("fslmaths {} -sub {} {}".format(allvoxelscortsubcort, a_file, exclusion), params)
        run("fslmaths {} -sub {} {}".format(exclusion, b_file, exclusion), params)
        run("fslmaths {} -add {} {}".format(exclusion, bs, exclusion), params)
        run("fslmaths {} -add {} {}".format(terminationmask, b_file, termination), params)

        pbtx_args = (" -x {} ".format(a_file) +
            # " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000" +
            " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P {}".format(pbtx_sample_count) +
            " --waypoints={} --avoid={} --stop={}".format(waypoints, exclusion, termination) +
            " --forcedir --opd --rseed={}".format(random.randint(1000,9999)) +
            " -s {}".format(merged) +
            " -m {}".format(nodif_brain_mask) +
            " --dir={}".format(tmp) +
            " --out={}".format(a_to_b_formatted)
            )
        run("probtrackx2" + pbtx_args, params)

        waytotal_count = 0
        if exists(waytotal):
            with open(waytotal, 'r') as f:
                waytotal_count = f.read().strip()
                fdt_tmp = join(connectome_dir, "{}_to_{}.fdt.tmp".format(a, b))
                smart_remove(fdt_tmp)
                run(f"fslmeants -i {join(tmp, a_to_b_formatted)} -m {b_file} | head -n 1 > {fdt_tmp}", params) # based on getconnectome script
                time.sleep(5)
                with open(fdt_tmp, 'r') as f2:
                    fdt_count = f2.read().strip()
                if not is_float(waytotal_count):
                    write(stdout, "Error: Failed to read waytotal_count value {} in {}".format(waytotal_count, edge))
                    continue
                if not is_float(fdt_count):
                    write(stdout, "Error: Failed to read fdt_count value {} in {}".format(fdt_count, edge))
                    continue
                edge_file = join(connectome_dir, "{}_to_{}.dot".format(a, b))
                smart_remove(edge_file)
                write(edge_file, "{} {} {} {}".format(a, b, waytotal_count, fdt_count))

                # Error check edge file
                with open(edge_file) as f:
                    line = f.read().strip()
                    if len(line) > 0: # ignore empty lines
                        chunks = [x.strip() for x in line.split(' ') if x]
                        if not (len(chunks) == 4 and is_float(chunks[2]) and is_float(chunks[3])):
                            write(stdout, "Error: Connectome {} has invalid edge {} to {}".format(edge_file, a, b))
                            continue
        else:
            write(stdout, 'Error: failed to find waytotal for {} to {}'.format(a, b))
        copyfile(join(tmp, a_to_b_formatted), a_to_b_file) # keep edi output
        if not a == "lh.paracentral": # discard all temp files except these for debugging
            smart_remove(tmp)

@python_app(executors=['worker'])
def combine(params, inputs=[]):

    sdir = params['work_dir']
    stdout = params['stdout']
    pbtx_sample_count = params['pbtx_sample_count']
    pbtx_edges = get_edges_from_file(join(params['script_dir'], EDGE_LIST))
    connectome_idx_list = join(params['script_dir'], 'data/lists/connectome_idxs.txt')
    start_time = time.time()
    connectome_dir = join(sdir,"EDI","CNTMresults")
    oneway_list = join(sdir, "connectome_{}samples_oneway.txt".format(pbtx_sample_count))
    twoway_list = join(sdir, "connectome_{}samples_twoway.txt".format(pbtx_sample_count))
    oneway_nof = join(sdir, "connectome_{}samples_oneway_nof.mat".format(pbtx_sample_count)) # nof = number of fibers
    twoway_nof = join(sdir, "connectome_{}samples_twoway_nof.mat".format(pbtx_sample_count))
    oneway_nof_normalized = join(sdir, "connectome_{}samples_oneway_nofn.mat".format(pbtx_sample_count)) # nofn = number of fibers, normalized
    twoway_nof_normalized = join(sdir, "connectome_{}samples_twoway_nofn.mat".format(pbtx_sample_count))
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")
    edi_maps = join(sdir,"EDI","EDImaps")
    edge_total = join(edi_maps,"FAtractsumsTwoway.nii.gz")
    tract_total = join(edi_maps,"FAtractsumsRaw.nii.gz")
    smart_remove(oneway_list)
    smart_remove(twoway_list)
    smart_remove(oneway_nof_normalized)
    smart_remove(twoway_nof_normalized)
    smart_remove(oneway_nof)
    smart_remove(twoway_nof)
    smart_remove(edi_maps)
    smart_mkdir(pbtk_dir)
    smart_mkdir(consensus_dir)
    smart_mkdir(edi_maps)
    oneway_edges = {}
    twoway_edges = {}

    consensus_edges = []
    for edge in pbtx_edges:
        a, b = edge
        if [a, b] in consensus_edges or [b, a] in consensus_edges:
            continue
        consensus_edges.append(edge)

    copyfile(connectome_idx_list, join(sdir, 'connectome_idxs.txt')) # give each subject a copy for reference

    ##################################
    # Compile connectome matrices
    ##################################
    vol_idxs = {}
    with open(connectome_idx_list) as f:
        lines = [x.strip() for x in f.readlines() if x]
        max_idx = -1
        for line in lines:
            vol, idx = line.split(',', 1)
            idx = int(idx)
            vol_idxs[vol] = idx
            if idx > max_idx:
                max_idx = idx
        oneway_nof_normalized_matrix = np.zeros((max_idx+1, max_idx+1))
        oneway_nof_matrix = np.zeros((max_idx+1, max_idx+1))
        twoway_nof_normalized_matrix = np.zeros((max_idx+1, max_idx+1))
        twoway_nof_matrix = np.zeros((max_idx+1, max_idx+1))

    for edge in pbtx_edges:
        a, b = edge
        edge_file = join(connectome_dir, "{}_to_{}.dot".format(a, b))
        with open(edge_file) as f:
            chunks = [x.strip() for x in f.read().strip().split(' ') if x]
            a_to_b = (chunks[0], chunks[1])
            b_to_a = (chunks[1], chunks[0])
            waytotal_count = float(chunks[2])
            fdt_count = float(chunks[3])
            if b_to_a in twoway_edges:
                twoway_edges[b_to_a][0] += waytotal_count
                twoway_edges[b_to_a][1] += fdt_count
            else:
                twoway_edges[a_to_b] = [waytotal_count, fdt_count]
            oneway_edges[a_to_b] = [waytotal_count, fdt_count]

    for a_to_b in oneway_edges:
        a = a_to_b[0]
        b = a_to_b[1]
        for vol in a_to_b:
            if vol not in vol_idxs:
                write(stdout, 'Error: could not find {} in connectome idxs'.format(vol))
                break
        else:
            write(oneway_list, "{} {} {} {}".format(a, b, oneway_edges[a_to_b][0], oneway_edges[a_to_b][1]))
            oneway_nof_matrix[vol_idxs[a]][vol_idxs[b]] = oneway_edges[a_to_b][0]
            oneway_nof_normalized_matrix[vol_idxs[a]][vol_idxs[b]] = oneway_edges[a_to_b][1]

    for a_to_b in twoway_edges:
        a = a_to_b[0]
        b = a_to_b[1]
        for vol in a_to_b:
            if vol not in vol_idxs:
                write(stdout, 'Error: could not find {} in connectome idxs'.format(vol))
                break
        else:
            write(twoway_list, "{} {} {} {}".format(a, b, twoway_edges[a_to_b][0], twoway_edges[a_to_b][1]))
            twoway_nof_matrix[vol_idxs[a]][vol_idxs[b]] = twoway_edges[a_to_b][0]
            twoway_nof_normalized_matrix[vol_idxs[a]][vol_idxs[b]] = twoway_edges[a_to_b][1]
    scipy.io.savemat(oneway_nof, {'data': oneway_nof_matrix})
    scipy.io.savemat(oneway_nof_normalized, {'data': oneway_nof_normalized_matrix})
    scipy.io.savemat(twoway_nof, {'data': twoway_nof_matrix})
    scipy.io.savemat(twoway_nof_normalized, {'data': twoway_nof_normalized_matrix})
    smart_copy(twoway_nof_normalized, join(dirname(sdir), basename(twoway_nof_normalized)))
    smart_copy(twoway_list, join(dirname(sdir), basename(twoway_list)))
    
    ##################################
    # EDI consensus
    ##################################
    for edge in pbtx_edges:
        a, b = edge
        a_to_b = "{}_to_{}".format(a, b)
        a_to_b_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(a,b))
        b_to_a_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(b,a))
        if not exists(a_to_b_file):
            write(stdout, "Error: cannot find {}".format(a_to_b_file))
            return
        if not exists(b_to_a_file):
            write(stdout, "Error: cannot find {}".format(b_to_a_file))
            return
        consensus = join(consensus_dir, a_to_b + '.nii.gz')
        
        amax_tmp = join(connectome_dir, f"{a_to_b}.amax.tmp")
        bmax_tmp = join(connectome_dir, f"{a_to_b}.bmax.tmp")
        smart_remove(amax_tmp)
        smart_remove(bmax_tmp)
        run(f'fslstats {a_to_b_file} -R | cut -f 2 -d \\" \\" > {amax_tmp}', params).strip()
        run(f'fslstats {b_to_a_file} -R | cut -f 2 -d \\" \\" > {bmax_tmp}', params).strip()
        time.sleep(5)
        with open(amax_tmp, 'r') as f:
            amax = f.read().strip()
        with open(bmax_tmp, 'r') as f:
            bmax = f.read().strip()

        if not is_float(amax):
            write(stdout, "Error: fslstats on {} returns invalid value {}".format(a_to_b_file, amax))
            return
        amax = int(float(amax))

        if not is_float(bmax):
            write(stdout, "Error: fslstats on {} returns invalid value {}".format(b_to_a_file, bmax))
            return
        bmax = int(float(bmax))

        write(stdout, "amax = {}, bmax = {}".format(amax, bmax))
        if amax > 0 and bmax > 0:
            tmp1 = join(pbtk_dir, "{}_to_{}_tmp1.nii.gz".format(a, b))
            tmp2 = join(pbtk_dir, "{}_to_{}_tmp2.nii.gz".format(b, a))
            run("fslmaths {} -thrP 5 -bin {}".format(a_to_b_file, tmp1), params)
            run("fslmaths {} -thrP 5 -bin {}".format(b_to_a_file, tmp2), params)
            run("fslmaths {} -add {} -thr 1 -bin {}".format(tmp1, tmp2, consensus), params)
            smart_remove(tmp1)
            smart_remove(tmp2)
        else:
            with open(join(pbtk_dir, "zerosl.txt"), 'a') as log:
                log.write("For edge {}:\n".format(a_to_b))
                log.write("{} is thresholded to {}\n".format(a, amax))
                log.write("{} is thresholded to {}\n".format(b, bmax))

    # Collect number of probtrackx tracts per voxel
    for edge in pbtx_edges:
        a, b = edge
        a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
        a_to_b_file = join(pbtk_dir,a_to_b_formatted)
        if not exists(tract_total):
            copyfile(a_to_b_file, tract_total)
        else:
            run("fslmaths {0} -add {1} {1}".format(a_to_b_file, tract_total), params)

    # Collect number of parcel-to-parcel edges per voxel
    for edge in consensus_edges:
        a, b = edge
        consensus = join(consensus_dir, "{}_to_{}.nii.gz".format(a,b))
        if not exists(consensus):
            write(stdout,"{} has been thresholded. See {} for details".format(edge, join(pbtk_dir, "zerosl.txt")))
            continue
        if not exists(edge_total):
            copyfile(consensus, edge_total)
        else:
            run("fslmaths {0} -add {1} {1}".format(consensus, edge_total), params)
    if not exists(edge_total):
        write(stdout, "Error: Failed to generate {}".format(edge_total))
    else:
        smart_copy(edge_total, join(dirname(sdir), 'EDI_' + basename(edge_total)))

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
