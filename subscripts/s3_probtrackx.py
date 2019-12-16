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
        write(stdout, "Initializing Probtrackx with GPU")
    else:
        write(stdout, "Initializing Probtrackx without GPU")

@python_app(executors=['s3'], cache=True)
def s3_2_probtrackx(params, edges, inputs=[]):
    import time,platform,json,random,tempfile,os,fcntl,errno
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,is_integer,record_start,record_apptime,is_integer
    from os.path import join,exists,split,dirname
    from shutil import copyfile
    start_time = time.time()
    sdir = params['sdir']
    odir = split(sdir)[0]
    tmp_odir = join(odir, "tmp")
    tmp_sdir = join(sdir, "tmp")
    stdout = params['stdout']
    container = params['container']
    use_gpu = params['use_gpu']
    pbtx_sample_count = int(params['pbtx_sample_count'])
    pbtx_max_memory = float(params['pbtx_max_memory'])
    subject_random_seed = params['subject_random_seed']
    EDI_allvols = join(sdir,"EDI","allvols")
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bs = join(sdir,"bs.nii.gz")

    assert exists(bedpostxResults), "Could not find {}".format(bedpostxResults)

    ### Memory Management ###

    node_name = platform.uname().node.strip()
    assert node_name and ' ' not in node_name, "Invalid node name {}".format(node_name)
    mem_record = join(tmp_odir, node_name + '.json') # Keep record to avoid overusing node memory
    smart_mkdir(tmp_sdir)

    # Only access mem_record with file locking to avoid outdated data
    def open_mem_record(mode = 'r'):
        f = None
        while True:
            try:
                f = open(mem_record, mode, newline='')
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except IOError as e:
                # raise on unrelated IOErrors
                if e.errno != errno.EAGAIN:
                    raise
                else:
                    time.sleep(0.1)
        assert f is not None, "Failed to open mem_record {}".format(mem_record)
        return f

    def estimate_total_memory_usage():
        f = open_mem_record('r')
        mem_dict = json.load(f)
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()
        mem_sum = 0.0
        for task_mem in mem_dict.values():
            mem_sum += float(task_mem)
        return mem_sum

    def estimate_task_mem_usage():
        total_size = 0
        total_size += os.path.getsize(allvoxelscortsubcort)
        total_size += os.path.getsize(terminationmask)
        total_size += os.path.getsize(bs)
        for dirpath, dirnames, filenames in os.walk(bedpostxResults):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

        max_region_size = 0
        for edge in edges:
            a, b = edge
            a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
            b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
            a_size = os.path.getsize(a_file)
            b_size = os.path.getsize(b_file)
            max_region_size = max([a_size, b_size, max_region_size])
        total_size += max_region_size
        return float(total_size) * 1.0E-9

    def add_task():
        task_id = '0'
        f = open_mem_record('r')
        if not exists(mem_record):
            json.dump({task_id:task_mem_usage}, f)
        else:
            mem_dict = json.load(f)
            task_ids = [int(x) for x in mem_dict.keys()] + [0] # append zero in case task_ids empty
            task_id = str(max(task_ids) + 1) # generate incremental task_id
            mem_dict[task_id] = task_mem_usage
            tmp_fp, tmp_path = tempfile.mkstemp(dir=tmp_sdir)
            with open(tmp_path, 'w', newline='') as tmp: # file pointer not consistent, so we open using the pathname
                json.dump(mem_dict, tmp)
            os.replace(tmp_path, mem_record) # atomic on POSIX systems. flock is advisory, so we can still overwrite.
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()
        return task_id

    def remove_task(task_id):
        f = open_mem_record('r')
        mem_dict = json.load(f)
        mem_dict.pop(task_id, None)
        tmp_fp, tmp_path = tempfile.mkstemp(dir=tmp_sdir)
        with open(tmp_path, 'w', newline='') as tmp:
            json.dump(mem_dict, tmp)
        os.replace(tmp_path, mem_record)
        fcntl.flock(f, fcntl.LOCK_UN)
        f.close()

    if pbtx_max_memory > 0:
        if not exists(mem_record):
            f = open_mem_record('w')
            json.dump({}, f)
            fcntl.flock(f, fcntl.LOCK_UN)
            f.close()

        task_mem_usage = estimate_task_mem_usage()
        total_sleep = 0
        # Memory record is atomic, but might not be updated on time
        # So we randomize sleep to discourage multiple tasks hitting at once
        init_sleep = random.randrange(0, 120)
        write(stdout, "Sleeping for {:d} seconds".format(init_sleep))
        total_sleep += init_sleep
        time.sleep(init_sleep)

        total_mem_usage = estimate_total_memory_usage()
        # Then we sleep until memory usage is low enough
        while total_mem_usage + task_mem_usage > pbtx_max_memory:
            sleep_interval = random.randrange(30, 120)
            write(stdout, "Sleeping for {:d} seconds. Memory usage: {:.2f}/{:.2f} GB".format(sleep_interval, total_mem_usage, pbtx_max_memory))
            total_sleep += sleep_interval
            time.sleep(sleep_interval)
            total_mem_usage = estimate_total_memory_usage()
        write(stdout, "Running Probtrackx after sleeping for {} seconds".format(total_sleep))

        # Insert task and memory usage into record
        task_id = add_task()

    ### Probtrackx Run ###

    try:
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
                    assert is_float(waytotal_count), "Failed to read waytotal_count value {}".format(waytotal_count)
                    assert is_float(fdt_count), "Failed to read fdt_count value {}".format(fdt_count)
                    edge_file = join(connectome_dir, "{}_to_{}.dot".format(a, b))
                    smart_remove(edge_file)
                    write(edge_file, "{} {} {} {}".format(a, b, waytotal_count, fdt_count))

                    # Error check edge file
                    with open(edge_file) as f:
                        line = f.read().strip()
                        if len(line) > 0: # ignore empty lines
                            chunks = [x.strip() for x in line.split(' ') if x]
                            assert len(chunks) == 4 and is_float(chunks[2]) and is_float(chunks[3]), "Connectome {} has invalid edge {} to {}".format(edge_file, a, b)
            else:
                write(stdout, 'Error: failed to find waytotal for {} to {}'.format(a, b))
            copyfile(join(tmp, a_to_b_formatted), a_to_b_file) # keep edi output
            if not a == "lh.paracentral": # discard all temp files except these for debugging
                smart_remove(tmp)
    finally:
        if pbtx_max_memory > 0:
            remove_task(task_id)

    record_apptime(params, start_time, 1)

@python_app(executors=['s3'], cache=True)
def s3_3_combine(params, inputs=[]):
    import numpy as np
    import scipy.io
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions,is_float,write,get_edges_from_file
    from os.path import join,exists
    from shutil import copyfile
    sdir = params['sdir']
    stdout = params['stdout']
    pbtx_sample_count = int(params['pbtx_sample_count'])
    pbtx_edge_list = params['pbtx_edge_list']
    connectome_idx_list = params['connectome_idx_list']
    connectome_idx_list_copy = join(sdir, 'connectome_idxs.txt')
    start_time = time.time()
    connectome_dir = join(sdir,"EDI","CNTMresults")
    oneway_list = join(sdir, "connectome_{}samples_oneway.txt".format(pbtx_sample_count))
    twoway_list = join(sdir, "connectome_{}samples_twoway.txt".format(pbtx_sample_count))
    oneway_nof = join(sdir, "connectome_{}samples_oneway_nof.mat".format(pbtx_sample_count)) # nof = number of fibers
    twoway_nof = join(sdir, "connectome_{}samples_twoway_nof.mat".format(pbtx_sample_count))
    oneway_nof_normalized = join(sdir, "connectome_{}samples_oneway_nofn.mat".format(pbtx_sample_count)) # nofn = number of fibers, normalized
    twoway_nof_normalized = join(sdir, "connectome_{}samples_twoway_nofn.mat".format(pbtx_sample_count))
    smart_remove(oneway_list)
    smart_remove(twoway_list)
    smart_remove(oneway_nof_normalized)
    smart_remove(twoway_nof_normalized)
    smart_remove(oneway_nof)
    smart_remove(twoway_nof)
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
        oneway_nof_normalized_matrix = np.zeros((max_idx+1, max_idx+1))
        oneway_nof_matrix = np.zeros((max_idx+1, max_idx+1))
        twoway_nof_normalized_matrix = np.zeros((max_idx+1, max_idx+1))
        twoway_nof_matrix = np.zeros((max_idx+1, max_idx+1))

    for edge in get_edges_from_file(pbtx_edge_list):
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

    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

@python_app(executors=['s3'], cache=True)
def s3_4_edi_consensus(params, edges, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,record_apptime
    from os.path import join,exists,split
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")

    for edge in edges:
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
        amax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(a_to_b_file), params).strip()
        if not is_float(amax):
            write(stdout, "Error: fslstats on {} returns invalid value {}".format(a_to_b_file, amax))
            return
        amax = int(float(amax))
        bmax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(b_to_a_file), params).strip()
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
    record_apptime(params, start_time, 3)

@python_app(executors=['s3'], cache=True)
def s3_5_edi_combine(params, consensus_edges, inputs=[]):
    import time,tarfile
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,record_apptime,record_finish, \
                                     update_permissions,get_edges_from_file,strip_trailing_slash
    from os.path import join,exists,basename
    from shutil import copyfile
    pbtx_edge_list = params['pbtx_edge_list']
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    compress_pbtx_results = params['compress_pbtx_results']
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")
    edi_maps = join(sdir,"EDI","EDImaps")
    edge_total = join(edi_maps,"FAtractsumsTwoway.nii.gz")
    tract_total = join(edi_maps,"FAtractsumsRaw.nii.gz")

    # Collect number of probtrackx tracts per voxel
    for edge in get_edges_from_file(pbtx_edge_list):
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
    assert exists(edge_total), "Failed to generate {}".format(edge_total)

    if compress_pbtx_results:
        pbtk_archive = strip_trailing_slash(pbtk_dir) + '.tar.gz'
        connectome_archive = strip_trailing_slash(connectome_dir) + '.tar.gz'
        write(stdout,"\nCompressing probtrackx output at {} and {}".format(pbtk_archive, connectome_archive))
        smart_remove(pbtk_archive)
        smart_remove(connectome_archive)
        with tarfile.open(pbtk_archive, mode='w:gz') as archive:
            archive.add(pbtk_dir, recursive=True, arcname=basename(pbtk_dir))
        with tarfile.open(connectome_archive, mode='w:gz') as archive:
            archive.add(connectome_dir, recursive=True, arcname=basename(connectome_dir))
        smart_remove(pbtk_dir)
        smart_remove(connectome_dir)

    update_permissions(params)
    record_apptime(params, start_time, 4)
    record_finish(params)

def setup_s3(params, inputs):
    edge_chunk_size = 32 # set to >1, if number of jobs causes log output to crash
    sdir = params['sdir']
    stdout = params['stdout']
    pbtx_edge_list = params['pbtx_edge_list']
    pbtx_random_seed = params['pbtx_random_seed']
    params['subject_random_seed'] = random.randint(0, 999999) if pbtx_random_seed is None else pbtx_random_seed
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_dir = join(sdir,"EDI","CNTMresults")
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")
    edi_maps = join(sdir,"EDI","EDImaps")
    tmp_dir = join(sdir,"tmp")
    smart_remove(tmp_dir)
    smart_remove(pbtk_dir)
    smart_remove(connectome_dir)
    smart_remove(consensus_dir)
    smart_remove(edi_maps)
    smart_mkdir(tmp_dir)
    smart_mkdir(pbtk_dir)
    smart_mkdir(connectome_dir)
    smart_mkdir(consensus_dir)
    smart_mkdir(edi_maps)

    # Probtrackx
    s3_1_future = s3_1_start(params, inputs=inputs)
    s3_2_futures = []
    edges_chunk = []
    for edge in get_edges_from_file(pbtx_edge_list):
        edges_chunk.append(edge)
        if len(edges_chunk) >= edge_chunk_size:
            s3_2_futures.append(s3_2_probtrackx(params, edges_chunk, inputs=[s3_1_future]))
            edges_chunk = []
    if edges_chunk: # run last chunk if it's not empty
        s3_2_futures.append(s3_2_probtrackx(params, edges_chunk, inputs=[s3_1_future]))
    s3_3_future = s3_3_combine(params, inputs=s3_2_futures)

    # EDI consensus
    s3_4_futures = []
    edges_chunk = []
    consensus_edges = []
    for edge in get_edges_from_file(pbtx_edge_list):
        a, b = edge
        if [a, b] in consensus_edges or [b, a] in consensus_edges:
            continue
        edges_chunk.append(edge)
        consensus_edges.append(edge)
        if len(edges_chunk) >= edge_chunk_size:
            s3_4_futures.append(s3_4_edi_consensus(params, edges_chunk, inputs=[s3_3_future]))
            edges_chunk = []
    if edges_chunk: # run last chunk if it's not empty
        s3_4_futures.append(s3_4_edi_consensus(params, edges_chunk, inputs=[s3_3_future]))

    return s3_5_edi_combine(params, consensus_edges, inputs=s3_4_futures)
