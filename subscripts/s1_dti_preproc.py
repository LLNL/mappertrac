#!/usr/bin/env python3
from subscripts.utilities import run,is_integer,write,smart_copy
from os.path import join
from parsl.app.app import python_app
from shutil import copyfile

@python_app(executors=['s1'], cache=True)
def s1_1_dicom_preproc(params, inputs=[]):
    import time,tarfile
    from subscripts.utilities import run,record_apptime,record_start,smart_remove,smart_copy, \
                                     smart_mkdir,write,strip_trailing_slash
    from os.path import join,split,exists,basename
    from shutil import copyfile
    from glob import glob
    import numpy as np
    sdir = params['sdir']
    stdout = params['stdout']
    T1_dicom_dir = params['T1_dicom_dir']
    DTI_dicom_dir = params['DTI_dicom_dir']
    nifti_dir = params['nifti_dir']
    odir = split(sdir)[0]
    container = params['container']
    DTI_dicom_dir = params['DTI_dicom_dir']
    T1_dicom_dir = params['T1_dicom_dir']

    dicom_tmp_dir = join(sdir, 'raw_dicom_inputs')
    DTI_dicom_tmp_dir = join(dicom_tmp_dir, 'DTI')
    T1_dicom_tmp_dir = join(dicom_tmp_dir, 'T1')

    hardi_file = join(nifti_dir, "hardi.nii.gz")
    T1_file = join(nifti_dir, "anat.nii.gz")
    bvals_file = join(nifti_dir, "bvals")
    bvecs_file = join(nifti_dir, "bvecs")

    start_time = time.time()
    record_start(params)

    if T1_dicom_dir and DTI_dicom_dir:
        smart_remove(nifti_dir)
        smart_remove(DTI_dicom_tmp_dir)
        smart_remove(T1_dicom_tmp_dir)
        smart_copy(T1_dicom_dir, T1_dicom_tmp_dir, ['*.nii', '*.nii.gz', '*.bval', '*.bvec'])
        smart_copy(DTI_dicom_dir, DTI_dicom_tmp_dir, ['*.nii', '*.nii.gz', '*.bval', '*.bvec'])
        smart_mkdir(nifti_dir)

        # Run dcm2nii in script to ensure Singularity container finds the right paths
        dicom_sh = join(sdir, "dicom.sh")
        smart_remove(dicom_sh)

        dicom_sh_contents = "dcm2nii -4 N"
        for file in glob(join(DTI_dicom_tmp_dir, '*.dcm')):
            dicom_sh_contents += " " + file

        dicom_sh_contents += "\ndcm2nii"
        for file in glob(join(T1_dicom_tmp_dir, '*.dcm')):
            dicom_sh_contents += " " + file

        if container:
            write(dicom_sh, dicom_sh_contents.replace(odir, "/share"))
        else:
            write(dicom_sh, dicom_sh_contents)
        run("sh " + dicom_sh, params)

        b0_slices = []
        b0_slice_vals = []
        normal_slices = []
        all_slices = {}

        # Check that dcm2nii outputs exist
        found_bvals = glob(join(DTI_dicom_tmp_dir, '*.bval'))
        found_bvecs = glob(join(DTI_dicom_tmp_dir, '*.bvec'))
        found_T1 = glob(join(T1_dicom_tmp_dir, 'co*.nii.gz'))

        if len(found_bvals) != 1:
            raise Exception('Did not find exactly one bvals output in {}'.format(DTI_dicom_tmp_dir))
        else:
            copyfile(found_bvals[0], bvals_file)

        if len(found_bvecs) != 1:
            raise Exception('Did not find exactly one bvecs output in {}'.format(DTI_dicom_tmp_dir))
        else:
            copyfile(found_bvecs[0], bvecs_file)

        if len(found_T1) != 1:
            raise Exception('Did not find exactly one T1 output in {}'.format(T1_dicom_tmp_dir))
        else:
            copyfile(found_T1[0], T1_file)

        # Find and average the B0 values in DTI files
        for file in glob(join(DTI_dicom_tmp_dir, '*.nii.gz')):
            slice_val = run("fslmeants -i {} | head -n 1".format(file), params) # based on getconnectome script
            all_slices[file] = float(slice_val)
        slice_vals = list(all_slices.values())
        median = np.median(slice_vals)
        for key in all_slices.keys():
            slice_val = all_slices[key]
            # mark as B0 if difference from median is greater than 20% of the median slice value
            if abs(slice_val - median) > 0.2 * median:
                b0_slices.append(key)
                b0_slice_vals.append(slice_val)
            else:
                normal_slices.append(key)
        normal_slices.sort()

        if b0_slice_vals and np.std(b0_slice_vals) > 0.2 * median:
            raise Exception('Standard deviation of B0 values is greater than 20\% of the median slice value. ' +
                  'This probably means that this script has incorrectly identified B0 slices.')
        if not b0_slices:
            raise Exception('Failed to find B0 values in {}'.format(T1_dicom_dir))

        avg_b0 = join(DTI_dicom_tmp_dir, 'avg_b0.nii.gz')
        smart_remove(avg_b0)

        # Concatenate DTI files into a single hardi.nii.gz, with averaged B0 values as first timeslice
        for file in b0_slices:
            if not exists(avg_b0):
                copyfile(file, avg_b0)
            else:
                run("fslmaths {0} -add {1} {1}".format(file, avg_b0), params)
        run("fslmaths {0} -div {1} {0}".format(avg_b0, len(b0_slices)), params)
        run("fslmerge -t {} {}".format(hardi_file, " ".join([avg_b0] + normal_slices)), params)

        # Compress DICOM inputs
        dicom_tmp_archive = strip_trailing_slash(dicom_tmp_dir) + '.tar.gz'
        write(stdout,"\nCompressing dicom inputs at {}".format(dicom_tmp_archive))
        smart_remove(dicom_tmp_archive)
        with tarfile.open(dicom_tmp_archive, mode='w:gz') as archive:
            archive.add(dicom_tmp_dir, recursive=True, arcname=basename(dicom_tmp_dir))
        smart_remove(dicom_tmp_dir)

    record_apptime(params, start_time, 1)

### The following three functions parallelize FSL's "eddy_correct"
@python_app(executors=['s1'], cache=True)
def s1_2_split_timeslices(params, inputs=[]):
    import time
    from subscripts.utilities import run,record_apptime,smart_remove,smart_copy
    from os.path import join
    from glob import glob
    nifti_dir = params['nifti_dir']
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    output_prefix = join(sdir,"data_eddy")
    timeslices = glob("{}_tmp????.*".format(output_prefix))
    for i in timeslices:
        smart_remove(i)
    for j in glob("{}_ref*".format(output_prefix)):
        smart_remove(j)
    input_data = join(sdir, "hardi.nii.gz")
    smart_copy(join(nifti_dir,"hardi.nii.gz"),input_data)
    smart_copy(join(nifti_dir,"bvecs"),join(sdir,"bvecs"))
    smart_copy(join(nifti_dir,"bvals"),join(sdir,"bvals"))
    smart_copy(join(nifti_dir,"anat.nii.gz"),join(sdir,"T1.nii.gz"))
    output_prefix = join(sdir,"data_eddy")
    run("fslroi {} {}_ref 0 1".format(input_data, output_prefix), params)
    run("fslsplit {} {}_tmp".format(input_data, output_prefix), params)
    record_apptime(params, start_time, 2)

@python_app(executors=['s1'], cache=True)
def s1_3_timeslice_process(params, worker_id, num_workers, inputs=[]):
    import time
    from subscripts.utilities import run,record_apptime
    from os.path import join,exists
    sdir = params['sdir']
    stdout = params['stdout']
    container = params['container']
    start_time = time.time()
    output_prefix = join(sdir,"data_eddy")
    timeslice = worker_id
    slice_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(timeslice))
    iteration = 0
    while exists(slice_data):
        # Break loop if it gets stuck
        if iteration > 99:
            break
        run("flirt -in {0} -ref {1}_ref -nosearch -interp trilinear -o {0} -paddingsize 1".format(slice_data, output_prefix), params)
        # Example: worker #3 with 10 total workers will process timeslices 3, 13, 23, 33...
        timeslice += num_workers
        slice_data = join(sdir,"data_eddy_tmp{:04d}.nii.gz".format(timeslice))
        iteration += 1
    record_apptime(params, start_time, 3)

@python_app(executors=['s1'], cache=True)
def s1_4_dti_fit(params, inputs=[]):
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
    timeslices = glob("{}_tmp????.nii.gz".format(output_prefix))
    timeslices.sort()
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
    record_apptime(params, start_time, 4)
    record_finish(params)

def setup_s1(params, inputs):
    nifti_dir = params['nifti_dir']
    stdout = params['stdout']
    container = params['container']
    sdir = params['sdir']
    # input_data = join(sdir, "hardi.nii.gz")
    # smart_copy(join(nifti_dir, "hardi.nii.gz"), input_data)
    # timeslices = run("fslinfo {} | sed -n -e '/^dim4/p'".format(input_data), params).split()
    # if not timeslices or not is_integer(timeslices[-1]):
    #     write(stdout, "Failed to read timeslices from {}".format(input_data))
    #     return
    # num_timeslices = timeslices[-1]
    num_workers = 32
    s1_1_future = s1_1_dicom_preproc(params, inputs=inputs)
    s1_2_future = s1_2_split_timeslices(params, inputs=[s1_1_future])
    s1_3_futures = []
    for i in range(num_workers):
        s1_3_future = s1_3_timeslice_process(params, i, num_workers, inputs=[s1_2_future])
        s1_3_futures.append(s1_3_future)
    return s1_4_dti_fit(params, inputs=s1_3_futures)
