#!/usr/bin/env python3
import random
from os.path import join,exists
from subscripts.utilities import write,smart_remove,smart_mkdir,get_edges_from_file
from parsl.app.app import python_app

@python_app(executors=['s3'], cache=True)
def s3_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    use_gpu = params['use_gpu']
    stdout = params['stdout']
    record_start(params)
    if use_gpu:
        write(stdout, "Running Probtrackx with GPU")
    else:
        write(stdout, "Running Probtrackx without GPU")

@python_app(executors=['s3'], cache=True)
def s3_2_probtrackx(params, edges, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,is_integer,record_start,record_apptime
    from os.path import join,exists,split
    from shutil import copyfile
    start_time = time.time()
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    use_gpu = params['use_gpu']
    pbtx_sample_count = int(params['pbtx_sample_count'])
    subject_random_seed = params['subject_random_seed']
    EDI_allvols = join(sdir,"EDI","allvols")
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bs = join(sdir,"bs.nii.gz")

    for edge in edges:
        a, b = edge
        a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
        b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
        tmp = join(sdir, "tmp", "{}_to_{}".format(a, b))
        a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
        a_to_b_file = join(pbtk_dir,a_to_b_formatted)
        merged = join(bedpostxResults,"merged")
        nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
        waypoints = join(tmp,"waypoint.txt")
        waytotal = join(tmp, "waytotal")
        if not exists(a_file) or not exists(b_file):
            raise Exception("Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file))
        smart_remove(a_to_b_file)
        smart_remove(tmp)
        smart_mkdir(tmp)
        write(stdout, "Running subproc: {} to {}".format(a, b))
        if container:
            odir = split(sdir)[0]
            write(waypoints, b_file.replace(odir, "/share"))
        else:
            write(waypoints, b_file)

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
            " --forcedir --opd --rseed={}".format(subject_random_seed) +
            " -s {}".format(merged) +
            " -m {}".format(nodif_brain_mask) +
            " --dir={}".format(tmp) +
            " --out={}".format(a_to_b_formatted)
            )
        if use_gpu:
            probtrackx2_sh = join(tmp, "probtrackx2.sh")
            smart_remove(probtrackx2_sh)
            write(probtrackx2_sh, "export CUDA_LIB_DIR=$CUDA_8_LIB_DIR\n" +
                           "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:$LD_LIBRARY_PATH\n" +
                           "probtrackx2_gpu" + pbtx_args.replace(odir, "/share"))
            run("sh " + probtrackx2_sh, params)
        else:
            run("probtrackx2" + pbtx_args, params)

        waytotal_count = 0
        if exists(waytotal):
            with open(waytotal, 'r') as f:
                waytotal_count = f.read().strip()
                fdt_count = run("fslmeants -i {} -m {} | head -n 1".format(join(tmp, a_to_b_formatted), b_file), params) # based on getconnectome script
                if not is_float(waytotal_count):
                    raise Exception("Failed to read waytotal_count value {}".format(waytotal_count))
                if not is_float(fdt_count):
                    raise Exception("Failed to read fdt_count value {}".format(fdt_count))
                edge_file = join(connectome_dir, "{}_to_{}.dot".format(a, b))
                write(edge_file, "{} {} {} {}".format(a, b, waytotal_count, fdt_count))

                # Error check edge file
                if not exists(edge_file):
                    raise Exception("Failed to find connectome for edge {} to {}".format(a, b))
                with open(edge_file) as f:
                    chunks = [x.strip() for x in f.read().strip().split(' ') if x]
                    if len(chunks) != 4 or not is_float(chunks[2]) or not is_float(chunks[3]):
                        raise Exception('Connectome edge {} to {} has invalid line {}'.format(a, b, f.read().strip()))
        else:
            write(stdout, 'Error: failed to find waytotal for {} to {}'.format(a, b))
        copyfile(join(tmp, a_to_b_formatted), a_to_b_file) # keep edi output
        if not a == "lh.paracentral": # discard all temp files except these for debugging
            smart_remove(tmp)
    record_apptime(params, start_time, 1)

@python_app(executors=['s3'], cache=True)
def s3_3_combine(params, inputs=[]):
    import numpy as np
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions,is_float,write,get_edges_from_file
    from os.path import join,exists
    from shutil import copyfile
    sdir = params['sdir']
    stdout = params['stdout']
    edge_list = params['edge_list']
    connectome_idx_list = params['connectome_idx_list']
    connectome_idx_list_copy = join(sdir, 'connectome_idxs.txt')
    start_time = time.time()
    connectome_dir = join(sdir,"EDI","CNTMresults")
    connectome_oneway = join(sdir, "connectome_oneway.dot")
    connectome_twoway = join(sdir, "connectome_twoway.dot")
    connectome_oneway_mat = join(sdir, "connectome_oneway.mat")
    connectome_twoway_mat = join(sdir, "connectome_twoway.mat")
    smart_remove(join(sdir, "connectome_twoway.dot"))
    oneway_edges = {}
    twoway_edges = {}

    copyfile(connectome_idx_list, connectome_idx_list_copy) # give each subject a copy for reference

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
        oneway_matrix = np.zeros((max_idx+1, max_idx+1))
        twoway_matrix = np.zeros((max_idx+1, max_idx+1))

    for edge in get_edges_from_file(edge_list):
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
            write(connectome_oneway, "{} {} {} {}".format(a, b, oneway_edges[a_to_b][0], oneway_edges[a_to_b][1]))
            oneway_matrix[vol_idxs[a]][vol_idxs[b]] = oneway_edges[a_to_b][1]

    for a_to_b in twoway_edges:
        a = a_to_b[0]
        b = a_to_b[1]
        for vol in a_to_b:
            if vol not in vol_idxs:
                write(stdout, 'Error: could not find {} in connectome idxs'.format(vol))
                break
        else:
            write(connectome_twoway, "{} {} {} {}".format(a, b, twoway_edges[a_to_b][0], twoway_edges[a_to_b][1]))
            twoway_matrix[vol_idxs[a]][vol_idxs[b]] = twoway_edges[a_to_b][1]
    np.savetxt(connectome_oneway_mat, oneway_matrix, fmt='%g')
    np.savetxt(connectome_twoway_mat, twoway_matrix, fmt='%g')

    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def setup_s3(params, inputs):
    edge_chunk_size = 8 # set to >1, if number of jobs causes log output to crash
    sdir = params['sdir']
    stdout = params['stdout']
    edge_list = params['edge_list']
    pbtx_random_seed = params['pbtx_random_seed']
    params['subject_random_seed'] = random.randint(0, 999999) if pbtx_random_seed is None else pbtx_random_seed
    s3_1_future = s3_1_start(params, inputs=inputs)
    s3_2_futures = []
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    tmp_dir = join(sdir,"tmp")
    smart_remove(tmp_dir)
    smart_remove(pbtk_dir)
    smart_remove(connectome_dir)
    smart_mkdir(tmp_dir)
    smart_mkdir(pbtk_dir)
    smart_mkdir(connectome_dir)
    edges_chunk = []
    for edge in get_edges_from_file(edge_list):
        edges_chunk.append(edge)
        if len(edges_chunk) >= edge_chunk_size:
            s3_2_futures.append(s3_2_probtrackx(params, edges_chunk, inputs=[s3_1_future]))
            edges_chunk = []
    if edges_chunk: # run last chunk if it's not empty
        s3_2_futures.append(s3_2_probtrackx(params, edges_chunk, inputs=[s3_1_future]))
    return s3_3_combine(params, inputs=s3_2_futures)
