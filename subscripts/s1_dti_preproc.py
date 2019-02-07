#!/usr/bin/env python3
from subscripts.utilities import run,is_integer,write,smart_copy
from os.path import join
from parsl.app.app import python_app
from shutil import copyfile

### These three functions parallelize FSL's "eddy_correct"

@python_app(executors=['s1'], cache=True)
def s1_1_split_timeslices(params, inputs=[]):
    import time
    from subscripts.utilities import run,record_apptime,record_start,smart_remove,smart_copy
    from os.path import join
    from glob import glob
    input_dir = params['input_dir']
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    record_start(params)
    start_time = time.time()
    output_prefix = join(sdir,"data_eddy")
    timeslices = glob("{}_tmp????.*".format(output_prefix))
    for i in timeslices:
        smart_remove(i)
    for j in glob("{}_ref*".format(output_prefix)):
        smart_remove(j)
    input_data = join(sdir, "hardi.nii.gz")
    smart_copy(join(input_dir,"bvecs"),join(sdir,"bvecs"))
    smart_copy(join(input_dir,"bvals"),join(sdir,"bvals"))
    smart_copy(join(input_dir,"anat.nii.gz"),join(sdir,"T1.nii.gz"))
    output_prefix = join(sdir,"data_eddy")
    run("fslroi {} {}_ref 0 1".format(input_data, output_prefix), params)
    run("fslsplit {} {}_tmp".format(input_data, output_prefix), params)
    record_apptime(params, start_time, 1)

@python_app(executors=['s1'], cache=True)
def s1_2_timeslice_process(params, timeslice, inputs=[]):
    import time
    from subscripts.utilities import run,record_apptime
    from os.path import join,exists
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    slice_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(timeslice))
    if not exists(slice_data):
        raise Exception("Error: Failed to find timeslice {}".format(slice_data)) 
    output_prefix = join(sdir,"data_eddy")
    run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1".format(slice_data, output_prefix), params)
    record_apptime(params, start_time, 2)

@python_app(executors=['s1'], cache=True)
def s1_3_dti_fit(params, inputs=[]):
    import time
    from subscripts.utilities import run,smart_remove,record_apptime,record_finish,update_permissions
    from os.path import join,exists
    from shutil import copyfile
    from glob import glob
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    cores_per_task = params['cores_per_task']
    start_time = time.time()
    output_prefix = join(sdir,"data_eddy")
    output_data = join(sdir,"data_eddy.nii.gz")
    timeslices = glob("{}_tmp????.*".format(output_prefix))
    bet = join(sdir,"data_bet.nii.gz")
    bvecs = join(sdir,"bvecs")
    bvals = join(sdir,"bvals")
    bet_mask = join(sdir,"data_bet_mask.nii.gz")
    dti_params = join(sdir,"DTIparams")
    dti_L1 = dti_params + "_L1.nii.gz"
    dti_L2 = dti_params + "_L2.nii.gz"
    dti_L3 = dti_params + "_L3.nii.gz"
    dti_MD = dti_params + "_MD.nii.gz"
    dti_RD = dti_params + "_RD.nii.gz"
    dti_MD = dti_params + "_MD.nii.gz"
    dti_AD = dti_params + "_AD.nii.gz"
    dti_FA = dti_params + "_FA.nii.gz"
    FA = join(sdir,"FA.nii.gz")
    run("fslmerge -t {} {}".format(output_data, " ".join(timeslices)), params)
    run("bet {} {} -m -f 0.3".format(output_data,bet), params)
    run("dtifit --verbose -k {} -o {} -m {} -r {} -b {}".format(output_data,dti_params,bet_mask,bvecs,bvals), params)
    run("fslmaths {} -add {} -add {} -div 3 {}".format(dti_L1,dti_L2,dti_L3,dti_MD), params)
    run("fslmaths {} -add {} -div 2 {}".format(dti_L2,dti_L3,dti_RD), params)
    copyfile(dti_L1,dti_AD)
    copyfile(dti_FA,FA)
    for i in glob("{}_tmp????.*".format(output_prefix)):
        smart_remove(i)
    for j in glob("{}_ref*".format(output_prefix)):
        smart_remove(j)
    update_permissions(params)
    record_apptime(params, start_time, 3)
    record_finish(params)

def run_s1(params, inputs):
    input_dir = params['input_dir']
    stdout = params['stdout']
    container = params['container']
    sdir = params['sdir']
    input_data = join(sdir, "hardi.nii.gz")
    smart_copy(join(input_dir, "hardi.nii.gz"), input_data)
    timeslices = run("fslinfo {} | sed -n -e '/^dim4/p'".format(input_data), params).split()
    if not timeslices or not is_integer(timeslices[-1]):
        write(stdout, "Failed to read timeslices from {}".format(input_data))
        return
    num_timeslices = timeslices[-1]
    s1_1_future = s1_1_split_timeslices(params, inputs=inputs)
    s1_2_futures = []
    for i in range(int(num_timeslices)):
        s1_2_future = s1_2_timeslice_process(params, i, inputs=[s1_1_future])
        s1_2_futures.append(s1_2_future)
    return s1_3_dti_fit(params, inputs=s1_2_futures)
