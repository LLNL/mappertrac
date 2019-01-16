#!/usr/bin/env python3
from parsl.app.app import python_app

@python_app(executors=['s2a'], cache=True)
def s2a_bedpostx(params, inputs=[]):
    import time
    from subscripts.utilities import run,smart_mkdir,smart_remove,write,record_start,record_apptime,record_finish,update_permissions
    from os.path import exists,join,split
    from shutil import copyfile,rmtree
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    cores_per_task = params['cores_per_task']
    use_gpu = params['use_gpu']
    group = params['group']
    record_start(params)
    start_time = time.time()
    bedpostx = join(sdir,"bedpostx_b1000")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    th1 = join(bedpostxResults, "merged_th1samples")
    ph1 = join(bedpostxResults, "merged_ph1samples")
    th2 = join(bedpostxResults, "merged_th2samples")
    ph2 = join(bedpostxResults, "merged_ph2samples")
    dyads1 = join(bedpostxResults, "dyads1")
    dyads2 = join(bedpostxResults, "dyads2")
    brain_mask = join(bedpostxResults, "nodif_brain_mask")
    if exists(bedpostxResults):
        rmtree(bedpostxResults)
    smart_mkdir(bedpostx)
    smart_mkdir(bedpostxResults)
    copyfile(join(sdir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"))
    copyfile(join(sdir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"))
    copyfile(join(sdir,"bvals"),join(bedpostx,"bvals"))
    copyfile(join(sdir,"bvecs"),join(bedpostx,"bvecs"))

    if use_gpu:
        write(stdout, "Running Bedpostx with GPU")
        bedpostx_sh = join(sdir, "bedpostx.sh")
        smart_remove(bedpostx_sh)
        odir = split(sdir)[0]
        write(bedpostx_sh, "export CUDA_LIB_DIR=$CUDA_8_LIB_DIR\n" +
                           "export LD_LIBRARY_PATH=$CUDA_LIB_DIR:$LD_LIBRARY_PATH")
        if container:
            write(bedpostx_sh, "bedpostx_gpu {} -NJOBS 4".format(bedpostx.replace(odir, "/share")))
        else:
            write(bedpostx_sh, "bedpostx_gpu {} -NJOBS 4".format(bedpostx))
        run("sh " + bedpostx_sh, params)
    else:
        write(stdout, "Running Bedpostx without GPU")
        run("bedpostx {}".format(bedpostx), params)
    run("make_dyadic_vectors {} {} {} {}".format(th1,ph1,brain_mask,dyads1), params)
    run("make_dyadic_vectors {} {} {} {}".format(th2,ph2,brain_mask,dyads2), params)
    update_permissions(params)
    record_apptime(params, start_time, 1)
    record_finish(params)

def run_s2a(params, inputs=[]):
    return s2a_bedpostx(params, inputs=inputs)
