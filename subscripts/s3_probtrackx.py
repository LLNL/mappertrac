#!/usr/bin/env python3
from os.path import join,exists
from subscripts.config import one_core_executor_labels
from subscripts.utilities import write,smart_remove
from parsl.app.app import python_app

@python_app(executors=one_core_executor_labels, cache=True)
def s3_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    use_gpu = params['use_gpu']
    record_start(params)
    if use_gpu:
        write(stdout, "Running Probtrackx with GPU")
    else:
        write(stdout, "Running Probtrackx without GPU")

@python_app(executors=['two_core'], cache=True)
def s3_2_probtrackx(params, a, b, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,is_integer,record_start,record_apptime
    from os.path import join,exists
    from shutil import copyfile
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    use_gpu = params['use_gpu']
    start_time = time.time()
    EDI_allvols = join(sdir,"EDI","allvols")
    a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
    b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
    a_to_b = "{}to{}".format(a, b)
    a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    connectome_inputs = join(pbtk_dir, "connectome")
    a_to_b_file = join(pbtk_dir,a_to_b_formatted)
    tmp = join(sdir, "tmp", a_to_b)
    waypoints = join(tmp,"waypoint.txt")
    exclusion = join(tmp,"exclusion.nii.gz")
    termination = join(tmp,"termination.nii.gz")
    bs = join(sdir,"bs.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    fdt_matrix1 = join(tmp, "fdt_matrix1.dot")
    waytotal = join(tmp, "waytotal")
    connectome = join(sdir, "connectome_oneway.dot")
    if not exists(a_file) or not exists(b_file):
        write(stdout, "Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file))
        return
    smart_remove(a_to_b_file)
    smart_remove(tmp)
    smart_mkdir(tmp)
    smart_mkdir(pbtk_dir)
    smart_mkdir(connectome_inputs)
    write(stdout, "Running subproc: {}".format(a_to_b))
    run("fslmaths {} -sub {} {}".format(allvoxelscortsubcort, a_file, exclusion), stdout, container)
    run("fslmaths {} -sub {} {}".format(exclusion, b_file, exclusion), stdout, container)
    run("fslmaths {} -add {} {}".format(exclusion, bs, exclusion), stdout, container)
    run("fslmaths {} -add {} {}".format(terminationmask, b_file, termination), stdout, container)
    with open(waypoints,"w") as f:
        f.write(b_file + "\n")
    arguments = (" -x {} ".format(a_file) +
        " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000" +
        " --waypoints={} --avoid={} --stop={}".format(waypoints, exclusion, termination) +
        " --forcedir --opd" +
        " -s {}".format(merged) +
        " -m {}".format(nodif_brain_mask) +
        " --dir={}".format(tmp) +
        " --out={}".format(a_to_b_formatted) +
        " --omatrix1" # output seed-to-seed sparse connectivity matrix
        )
    if use_gpu:
        run("probtrackx2_gpu" + arguments, stdout, container)
    else:
        run("probtrackx2" + arguments, stdout, container)
    if exists(fdt_matrix1) and exists(waytotal):
        waytotal_count = 0
        with open(waytotal, 'r') as f:
            count = f.read().strip()
            if is_integer(count):
                waytotal_count = int(count)
        matrix1_count = 0.0
        with open(fdt_matrix1, 'r') as f:
            for line in f.readlines():
                if not line:
                    continue
                chunks = [x for x in line.strip().split(' ') if x]
                if len(chunks) == 3 and is_float(chunks[2]):
                    matrix1_count += float(chunks[2])
        write(connectome, "{} {} {} {}".format(a, b, waytotal_count, matrix1_count))
    copyfile(join(tmp, a_to_b_formatted), a_to_b_file)
    if not a == "lh.paracentral": # keep for debugging
        smart_remove(tmp)
    record_apptime(params, start_time, 1, a, b)

@python_app(executors=one_core_executor_labels, cache=True)
def s3_3_combine(params, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions
    from os.path import join,exists
    sdir = params['sdir']
    start_time = time.time()
    connectome = join(sdir, "connectome_oneway.dot")
    connectome_twoway = join(sdir, "connectome_twoway.dot")
    smart_remove(connectome_twoway)
    processed_edges = {}
    if exists(connectome):
        with open(connectome,'r') as f:
            for line in f.readlines():
                if not line:
                    continue
                chunks = [x.strip() for x in line.strip().split(' ') if x]
                if len(chunks) == 3 and is_float(chunks[2]):
                    a_to_b = (chunks[0], chunks[1])
                    b_to_a = (chunks[1], chunks[0])
                    track_count = float(chunks[2])
                    if b_to_a in processed_edges:
                        processed_edges[b_to_a] += track_count
                    else:
                        processed_edges[a_to_b] = track_count
    with open(connectome_twoway,'a') as f:
        for a_to_b in processed_edges:
            f.write("{} {} {}".format(a_to_b[0], a_to_b[1], processed_edges[a_to_b]))
    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def run_s3(params, inputs):
    sdir = params['sdir']
    stdout = params['stdout']
    edge_list = params['edge_list']
    s3_1_future = s3_1_start(params, inputs=inputs)
    s3_2_futures = []
    processed_edges = []
    smart_remove(join(sdir, "connectome.dot"))
    smart_remove(join(sdir, "tmp"))
    with open(edge_list) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            a, b = edge.replace("_s2fa", "").strip().split(',', 1)
            a_to_b = "{}to{}".format(a, b)
            b_to_a = "{}to{}".format(b, a)
            if a_to_b not in processed_edges and b_to_a not in processed_edges:
                s3_2_futures.append(s3_2_probtrackx(params, a, b, inputs=[s3_1_future]))
                s3_2_futures.append(s3_2_probtrackx(params, b, a, inputs=[s3_1_future]))
                processed_edges.append(a_to_b)
                processed_edges.append(b_to_a)
    return s3_3_combine(params, inputs=s3_2_futures)
