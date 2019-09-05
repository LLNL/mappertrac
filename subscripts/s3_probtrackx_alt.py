#!/usr/bin/env python3
from os.path import join,exists
from subscripts.utilities import write,smart_remove,smart_mkdir,get_edges_from_file
from shutil import copyfile
from parsl.app.app import python_app

@python_app(executors=['s3'], cache=True)
def s3_1_start(params, volumes, inputs=[]):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,record_start,add_binary_vol,sub_binary_vol
    from os.path import join
    from shutil import copyfile
    sdir = params['sdir']
    use_gpu = params['use_gpu']
    stdout = params['stdout']
    record_start(params)
    if use_gpu:
        write(stdout, "Running Probtrackx with GPU")
    else:
        write(stdout, "Running Probtrackx without GPU")
    outdir = join(sdir, 'fast_outdir')
    smart_remove(outdir)
    smart_mkdir(outdir)

    EDI_allvols = join(sdir,"EDI","allvols")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bs = join(sdir,"bs.nii.gz")
    exclusion = join(outdir, "exclusion.nii.gz")
    termination = join(outdir, "termination.nii.gz")
    copyfile(allvoxelscortsubcort, exclusion)
    copyfile(terminationmask, termination)
    for vol in volumes:
        vol_file = join(EDI_allvols, vol + "_s2fa.nii.gz")
        if not exists(vol_file):
            raise Exception('Failed to find volume {}'.format(vol_file))
        sub_binary_vol(vol_file, exclusion, params)
        add_binary_vol(vol_file, termination, params)
    run("fslmaths {} -add {} {}".format(exclusion, bs, exclusion), params)

    # run_vtk = join(sdir, 'run_vtk.py')
    # run('/opt/vtk/bin/vtkpython {} {} {} {}'.format(run_vtk, termination, termination + '.png', 256), params)
    # run('/opt/vtk/bin/vtkpython {} {} {} {}'.format(run_vtk, exclusion, exclusion + '.png', 256), params)

@python_app(executors=['s3'], cache=True)
def s3_2_probtrackx(params, vol, volumes, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,record_start,record_apptime,sub_binary_vol
    from os.path import join,exists,split
    from shutil import copyfile
    start_time = time.time()
    sdir = params['sdir']
    use_gpu = params['use_gpu']
    pbtx_sample_count = int(params['pbtx_sample_count'])
    connectome_oneway = join(sdir, "connectome_oneway.dot")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    outdir = join(sdir, 'fast_outdir')
    exclusion = join(outdir, "exclusion.nii.gz")
    termination = join(outdir, "termination.nii.gz")
    EDI_allvols = join(sdir,"EDI","allvols")

    vol_file = join(EDI_allvols, vol + "_s2fa.nii.gz")
    if not exists(vol_file):
        raise Exception('Failed to find volume {}'.format(vol_file))
    vol_outdir = join(outdir, vol)
    smart_remove(vol_outdir)
    smart_mkdir(vol_outdir)
    waypoints = join(vol_outdir, 'waypoint.txt')
    for vol2 in volumes:
        if vol != vol2:
            vol2_file = join(EDI_allvols, vol2 + "_s2fa.nii.gz")
            if not exists(vol2_file):
                raise Exception('Failed to find volume {}'.format(vol2_file))
            write(waypoints, vol2_file, params)
    vol_termination = join(vol_outdir, "vol_termination.nii.gz")
    vol_exclusion = join(vol_outdir, "vol_exclusion.nii.gz")
    copyfile(termination, vol_termination)
    copyfile(exclusion, vol_exclusion)
    sub_binary_vol(vol_file, vol_termination, params)
    sub_binary_vol(vol_file, vol_exclusion, params)
    vol_formatted = "fdt_paths.nii.gz"

    pbtx_args = (" -x {} ".format(vol_file) +
                " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P {}".format(pbtx_sample_count) +
                " --waycond=OR --waypoints={}".format(waypoints) +
                " --os2t --s2tastext --targetmasks={}".format(waypoints) +
                " --stop={}".format(vol_termination) +
                " --avoid={}".format(vol_exclusion) +
                " --forcedir --opd" +
                " -s {}".format(merged) +
                " -m {}".format(nodif_brain_mask) +
                " --dir={}".format(vol_outdir) +
                " --out={}".format(vol_formatted)
                )
    if use_gpu:
        probtrackx2_sh = join(vol_outdir, "probtrackx2.sh")
        smart_remove(probtrackx2_sh)
        write(probtrackx2_sh, "export CUDA_LIB_DIR=$CUDA_8_LIB_DIR\n" +
                               "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:$LD_LIBRARY_PATH\n" +
                               "probtrackx2_gpu" + pbtx_args, params)
        run("sh " + probtrackx2_sh, params)
    else:
        run("probtrackx2" + pbtx_args, params)

    # vol_connectome = join(vol_outdir,"connectome.dot")
    # waytotal = join(vol_outdir, "waytotal")
    # if not exists(waytotal):
    #     write(stdout, 'Error: failed to find waytotal for volume {}'.format(vol))

    # with open(waytotal, 'r') as f:
    #     waytotal_count = f.read().strip()
    #     fdt_count = run("fslmeants -i {} -m {} | head -n 1".format(join(vol_outdir, vol_formatted), vol_file), params)
    #     if not is_float(waytotal_count):
    #         raise Exception("Failed to read waytotal_count value {}".format(waytotal_count))
    #     if not is_float(fdt_count):
    #         raise Exception("Failed to read fdt_count value {}".format(fdt_count))
    #     write(vol_connectome, "{} {} {}".format(vol, waytotal_count, fdt_count))

    record_apptime(params, start_time, 1)

@python_app(executors=['s3'], cache=True)
def s3_3_combine(params, volumes, inputs=[]):
    import time
    from subscripts.utilities import run,record_apptime,record_finish,update_permissions,write
    from os.path import join,exists
    sdir = params['sdir']
    # volumes = params['volumes']
    start_time = time.time()
    # run_vtk = join(sdir, 'run_vtk.py')
    outdir = join(sdir, 'fast_outdir')
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    total = join(outdir, 'FAtractsumsTwoway.nii.gz')
    run("fslmaths {} -mul 0 {}".format(allvoxelscortsubcort, total), params)
    for vol in volumes:
        vol_outdir = join(outdir, vol)
        pbtx_result = join(vol_outdir, 'fdt_paths.nii.gz')
        run("fslmaths {} -thrP 5 -bin {}".format(pbtx_result, pbtx_result), params)
        run("fslmaths {} -add {} {}".format(pbtx_result, total, total), params)

        waytotal = join(vol_outdir, "waytotal")
    # run('/opt/vtk/bin/vtkpython {} {} {} {}'.format(run_vtk, total, total + '.png', 256), params)

    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def setup_s3_alt(params, inputs):
    sdir = params['sdir']
    pbtx_edge_list = params['pbtx_edge_list']
    volumes = set([])
    for edge in get_edges_from_file(pbtx_edge_list):
        volumes.add(edge[0])
        volumes.add(edge[1])
    # run_vtk = join(sdir, 'run_vtk.py')
    # copyfile('subscripts/run_vtk.py', run_vtk)
    s3_1_future = s3_1_start(params, volumes, inputs=inputs)
    s3_2_futures = []
    for vol in volumes:
        s3_2_futures.append(s3_2_probtrackx(params, vol, volumes, inputs=[s3_1_future]))
    return s3_3_combine(params, volumes, inputs=s3_2_futures)
