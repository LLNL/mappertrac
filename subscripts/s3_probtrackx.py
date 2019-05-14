#!/usr/bin/env python3
from os.path import join,exists
from subscripts.utilities import write,smart_remove,smart_mkdir
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
    import time,random
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,is_integer,record_start,record_apptime
    from os.path import join,exists,split
    from shutil import copyfile
    start_time = time.time()
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    use_gpu = params['use_gpu']
    pbtx_sample_count = int(params['pbtx_sample_count'])
    pbtx_random_seed = params['pbtx_random_seed']
    if not pbtx_random_seed:
        pbtx_random_seed = random.randint(0, 999999)
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
        a_to_b = "{}_to_{}".format(a, b)
        a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
        a_to_b_file = join(pbtk_dir,a_to_b_formatted)
        merged = join(bedpostxResults,"merged")
        nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
        tmp = join(sdir, "tmp", a_to_b)
        waypoints = join(tmp,"waypoint.txt")
        waytotal = join(tmp, "waytotal")
        if not exists(a_file) or not exists(b_file):
            raise Exception("Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file))
        smart_remove(a_to_b_file)
        smart_remove(tmp)
        smart_mkdir(tmp)
        write(stdout, "Running subproc: {}".format(a_to_b))
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
            " --forcedir --opd --rseed={}".format(pbtx_random_seed) +
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
                connectome_oneway = join(connectome_dir, a_to_b + ".dot")
                write(connectome_oneway, "{} {} {} {}".format(a, b, waytotal_count, fdt_count))

                # Error check connectome output
                if not exists(connectome_oneway):
                    raise Exception("Failed to find connectome for edge {}".format(a_to_b))
                with open(connectome_oneway) as f:
                    chunks = [x.strip() for x in f.readlines().strip().split(' ') if x]
                    if len(chunks) != 4 or not is_float(chunks[2]) or not is_float(chunks[3]):
                        raise Exception('Connectome edge {} has invalid line {}'.format(a_to_b, line))
        else:
            write(stdout, 'Error: failed to find waytotal for {}'.format(a_to_b))
        copyfile(join(tmp, a_to_b_formatted), a_to_b_file) # keep edi output
        if not a == "lh.paracentral": # discard all temp files except these for debugging
            smart_remove(tmp)
    record_apptime(params, start_time, 1)

@python_app(executors=['s3'], cache=True)
def s3_3_combine(params, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions,is_float,write
    from os.path import join,exists
    sdir = params['sdir']
    stdout = params['stdout']
    edge_list = params['edge_list']
    start_time = time.time()
    connectome_twoway = join(sdir, "connectome_twoway.dot")
    smart_remove(join(sdir, "connectome_twoway.dot"))
    processed_edges = {}
    with open(edge_list) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            a, b = edge.replace("_s2fa", "").strip().split(',', 1)
            a_to_b = "{}_to_{}".format(a, b)
            connectome_oneway = join(connectome_dir, a_to_b + ".dot")
            chunks = [x.strip() for x in line.strip().split(' ') if x]
            if len(chunks) == 4 and is_float(chunks[2]) and is_float(chunks[3]):
                a_to_b = (chunks[0], chunks[1])
                b_to_a = (chunks[1], chunks[0])
                waytotal_count = float(chunks[2])
                fdt_count = float(chunks[3])
                if b_to_a in processed_edges:
                    processed_edges[b_to_a][0] += waytotal_count
                    processed_edges[b_to_a][1] += fdt_count
                else:
                    processed_edges[a_to_b] = [waytotal_count, fdt_count]
    for a_to_b in processed_edges:
        write(connectome_twoway, "{} {} {} {}".format(a_to_b[0], a_to_b[1], processed_edges[a_to_b][0], processed_edges[a_to_b][1]))
    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def setup_s3(params, inputs):
    edge_chunk_size = 1 # help with edge case, where number of jobs causes log output to crash
    sdir = params['sdir']
    stdout = params['stdout']
    edge_list = params['edge_list']
    s3_1_future = s3_1_start(params, inputs=inputs)
    s3_2_futures = []
    smart_remove(join(sdir, "connectome_oneway.dot"))
    smart_remove(join(sdir, "waytotal_oneway.dot"))
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    tmp_dir = join(sdir,"tmp")
    smart_remove(tmp_dir)
    smart_remove(pbtk_dir)
    smart_mkdir(tmp_dir)
    smart_mkdir(pbtk_dir)
    with open(edge_list) as f:
        edges = []
        for edge in f.readlines():
            if edge.isspace():
                continue
            a, b = edge.replace("_s2fa", "").strip().split(',', 1)
            edges.append([a, b])
            if len(edges) >= edge_chunk_size:
                s3_2_futures.append(s3_2_probtrackx(params, edges, inputs=[s3_1_future]))
                edges = []
        if edges:
            s3_2_futures.append(s3_2_probtrackx(params, edges, inputs=[s3_1_future]))
    return s3_3_combine(params, inputs=s3_2_futures)
