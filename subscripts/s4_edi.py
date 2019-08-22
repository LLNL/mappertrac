#!/usr/bin/env python3
from parsl.app.app import python_app
from os.path import join,exists
from subscripts.utilities import write,smart_remove,smart_mkdir,get_edges_from_file

@python_app(executors=['s4'], cache=True)
def s4_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    record_start(params)

@python_app(executors=['s4'], cache=True)
def s4_2_edi_consensus(params, a, b, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float,record_apptime
    from os.path import join,exists,split
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    a_to_b = "{}_to_{}".format(a, b)
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")
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
    record_apptime(params, start_time, 1, a, b)

@python_app(executors=['s4'], cache=True)
def s4_3_edi_combine(params, processed_edges, inputs=[]):
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
    for a_to_b in processed_edges:
        consensus = join(consensus_dir, a_to_b + ".nii.gz")
        if not exists(consensus):
            write(stdout,"{} has been thresholded. See {} for details".format(a_to_b, join(pbtk_dir, "zerosl.txt")))
            continue
        if not exists(edge_total):
            copyfile(consensus, edge_total)
        else:
            run("fslmaths {0} -add {1} {1}".format(consensus, edge_total), params)
    if not exists(edge_total):
        raise Exception("Error: Failed to generate {}".format(edge_total))

    if compress_pbtx_results:
        pbtk_archive = strip_trailing_slash(pbtk_dir) + '.tar.gz'
        connectome_archive = strip_trailing_slash(connectome_dir) + '.tar.gz'
        write(stdout,"Compressing probtrackx output at {} and {}".format(pbtk_archive, connectome_archive))
        smart_remove(pbtk_archive)
        smart_remove(connectome_archive)
        with tarfile.open(pbtk_archive, mode='w:gz') as archive:
            archive.add(pbtk_dir, recursive=True, arcname=basename(pbtk_dir))
        with tarfile.open(connectome_archive, mode='w:gz') as archive:
            archive.add(connectome_dir, recursive=True, arcname=basename(connectome_dir))
        smart_remove(pbtk_dir)
        smart_remove(connectome_dir)

    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def setup_s4(params, inputs):
    pbtx_edge_list = params['pbtx_edge_list']
    sdir = params['sdir']
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    consensus_dir = join(pbtk_dir,"twoway_consensus_edges")
    edi_maps = join(sdir,"EDI","EDImaps")
    smart_remove(consensus_dir)
    smart_remove(edi_maps)
    smart_mkdir(consensus_dir)
    smart_mkdir(edi_maps)
    s4_1_future = s4_1_start(params, inputs=inputs)
    s4_2_futures = []
    processed_edges = []
    for edge in get_edges_from_file(pbtx_edge_list):
        a, b = edge
        a_to_b = "{}_to_{}".format(a, b)
        b_to_a = "{}_to_{}".format(b, a)
        if a_to_b not in processed_edges and b_to_a not in processed_edges:
            s4_2_futures.append(s4_2_edi_consensus(params, a, b, inputs=[s4_1_future]))
            processed_edges.append(a_to_b)
    if not processed_edges:
        raise Exception("Error: Edge list is empty")
    return s4_3_edi_combine(params, processed_edges, inputs=s4_2_futures)
