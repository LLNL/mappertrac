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
    extra_b0_dirs = params['extra_b0_dirs']
    nifti_dir = params['nifti_dir']
    odir = split(sdir)[0]
    container = params['container']
    DTI_dicom_dir = params['DTI_dicom_dir']
    T1_dicom_dir = params['T1_dicom_dir']

    dicom_tmp_dir = join(sdir, 'raw_dicom_inputs')
    DTI_dicom_tmp_dir = join(dicom_tmp_dir, 'DTI')
    T1_dicom_tmp_dir = join(dicom_tmp_dir, 'T1')
    extra_b0_tmp_dirs = [join(dicom_tmp_dir, basename(dirname)) for dirname in extra_b0_dirs]

    hardi_file = join(nifti_dir, "hardi.nii.gz")
    T1_file = join(nifti_dir, "anat.nii.gz")
    bvals_file = join(nifti_dir, "bvals")
    bvecs_file = join(nifti_dir, "bvecs")

    start_time = time.time()
    record_start(params)

    if T1_dicom_dir and DTI_dicom_dir:
        smart_remove(nifti_dir)
        smart_mkdir(nifti_dir)
        smart_remove(DTI_dicom_tmp_dir)
        smart_remove(T1_dicom_tmp_dir)

        # copy everything from DICOM dir except old NiFTI outputs
        smart_copy(T1_dicom_dir, T1_dicom_tmp_dir, ['*.nii', '*.nii.gz', '*.bval', '*.bvec'])
        write(stdout, 'Copied {} to {}'.format(T1_dicom_dir, T1_dicom_tmp_dir))
        smart_copy(DTI_dicom_dir, DTI_dicom_tmp_dir, ['*.nii', '*.nii.gz', '*.bval', '*.bvec'])
        write(stdout, 'Copied {} to {}'.format(DTI_dicom_dir, DTI_dicom_tmp_dir))
        for (extra_b0_dir, extra_b0_tmp_dir) in zip(extra_b0_dirs, extra_b0_tmp_dirs):
            smart_remove(extra_b0_tmp_dir)
            smart_copy(extra_b0_dir, extra_b0_tmp_dir, ['*.nii', '*.nii.gz', '*.bval', '*.bvec'])
            write(stdout, 'Copied {} to {}'.format(extra_b0_dir, extra_b0_tmp_dir))

        # Run dcm2nii in script to ensure Singularity container finds the right paths
        dicom_sh = join(sdir, "dicom.sh")
        smart_remove(dicom_sh)

        # Convert DTI dicom to many individual NiFTI files
        dicom_sh_contents = "dcm2nii -4 N"
        for file in glob(join(DTI_dicom_tmp_dir, '*.dcm')):
            dicom_sh_contents += " " + file

        for extra_b0_tmp_dir in extra_b0_tmp_dirs:
            dicom_sh_contents += "\ndcm2nii -4 N"
            for file in glob(join(extra_b0_tmp_dir, '*.dcm')):
                dicom_sh_contents += " " + file

        dicom_sh_contents += "\ndcm2nii -4 N"
        for file in glob(join(T1_dicom_tmp_dir, '*.dcm')):
            dicom_sh_contents += " " + file

        if container:
            write(dicom_sh, dicom_sh_contents.replace(odir, "/share"))
        else:
            write(dicom_sh, dicom_sh_contents)
        write(stdout, 'Running dcm2nii with script {}'.format(dicom_sh))
        run("sh " + dicom_sh, params)

        b0_slices = {}
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

        found_T1.sort()
        if len(found_T1) == 0:
            raise Exception('Did not find T1 output in {}'.format(T1_dicom_tmp_dir))
        elif len(found_T1) > 1:
            write(stdout, 'Warning: Found more than one T1 output in {}'.format(T1_dicom_tmp_dir))
        copyfile(found_T1[0], T1_file)

        # Copy extra b0 values to DTI temp dir
        for extra_b0_tmp_dir in extra_b0_tmp_dirs:
            for file in glob(join(extra_b0_tmp_dir, "*.nii.gz")):
                copyfile(file, join(DTI_dicom_tmp_dir, "extra_b0_" + basename(file)))
            write(stdout, 'Copied NiFTI outputs from {} to {}'.format(extra_b0_tmp_dir, DTI_dicom_tmp_dir))

        # Sort slices into DTI and b0
        for file in glob(join(DTI_dicom_tmp_dir, '*.nii.gz')):
            slice_val = run("fslmeants -i {} | head -n 1".format(file), params) # based on getconnectome script
            all_slices[file] = float(slice_val)
        normal_median = np.median(list(all_slices.values()))
        for file in list(all_slices.keys()):
            slice_val = all_slices[file]
            # mark as b0 if more than 20% from normal slice median
            if abs(slice_val - normal_median) > 0.2 * normal_median:
                b0_slices[file] = slice_val
            else:
                normal_slices.append(file)
        if not b0_slices:
            raise Exception('Failed to find b0 values in {}'.format(DTI_dicom_dir))
        write(stdout, 'Found {} normal DTI slices'.format(len(normal_slices)))

        # Remove outliers from b0 values
        max_outliers = 1
        if len(b0_slices) > max_outliers:
            num_outliers = 0
            b0_median = np.median(list(b0_slices.values()))
            for file in list(b0_slices.keys()):
                slice_val = b0_slices[file]
                # remove outlier if more than 20% from b0 median
                if abs(slice_val - b0_median) > 0.2 * b0_median:
                    b0_slices.pop(file)
                    num_outliers += 1
            if num_outliers > max_outliers:
                raise Exception('Found more than {} outliers in b0 values. This probably means that this script has incorrectly identified b0 slices.'.format(max_outliers))
        write(stdout, 'Found {} b0 slices'.format(len(b0_slices)))

        # Average b0 slices into a single image
        avg_b0 = join(DTI_dicom_tmp_dir, 'avg_b0.nii.gz')
        smart_remove(avg_b0)
        for file in list(b0_slices.keys()):
            if not exists(avg_b0):
                copyfile(file, avg_b0)
            else:
                run("fslmaths {0} -add {1} {1}".format(file, avg_b0), params)
        run("fslmaths {0} -div {1} {0}".format(avg_b0, len(b0_slices)), params)

        # Concatenate average b0 and DTI slices into a single hardi.nii.gz
        normal_slices.sort()
        run("fslmerge -t {} {}".format(hardi_file, " ".join([avg_b0] + normal_slices)), params)
        write(stdout, 'Concatenated b0 and DTI slices into {}'.format(hardi_file))

        # Clean extra zeroes from bvals and bvecs files
        num_slices = len(normal_slices)
        with open(bvals_file, 'r+') as f:
            entries = [x.strip() for x in f.read().split() if x]
            extra_zero = entries.pop(0) # strip leading zero
            if extra_zero != "0":
                raise Exception("{} should begin with zero, as a placeholder for the averaged b0 slice".format(bvals_file))

            # remove zero sequences
            min_sequence_length = 5
            if all(x == "0" for x in entries[0:min_sequence_length]):
                write(stdout, "Stripped leading zero sequence from {}".format(bvals_file))
                while len(entries) > num_slices:
                    extra_zero = entries.pop(0)
                    if extra_zero != "0":
                        raise Exception("Failed to clean extra zeros from {}".format(bvals_file))
            elif all(x == "0" for x in entries[-1:-min_sequence_length-1:-1]):
                write(stdout, "Stripped trailing zero sequence from {}".format(bvals_file))
                while len(entries) > num_slices:
                    extra_zero = entries.pop(-1)
                    if extra_zero != "0":
                        raise Exception("Failed to clean extra zeros from {}".format(bvals_file))

            if len(entries) > num_slices:
                raise Exception('Failed to clean bvals file {}. Since {} has {} slices, bvals must have {} columns'.
                    format(bvals_file, hardi_file, num_slices, num_slices))
            text = "0 " + " ".join(entries) + "\n" # restore leading zero
            f.seek(0)
            f.write(text)
            f.truncate()
            write(stdout, 'Generated bvals file with values:\n{}'.format(text))
        with open(bvecs_file, 'r+') as f:
            text = ""
            for line in f.readlines():
                if not line:
                    continue
                entries = [x.strip() for x in line.split() if x]
                extra_zero = entries.pop(0) # strip leading zero
                if extra_zero != "0":
                    raise Exception("Each line in {} should begin with zero, as a placeholder for the averaged b0 slice".format(bvecs_file))

                # remove zero sequences
                min_sequence_length = 5
                if all(x == "0" for x in entries[0:min_sequence_length]):
                    write(stdout, "Stripped leading zero sequence from {}".format(bvecs_file))
                    while len(entries) > num_slices:
                        extra_zero = entries.pop(0)
                        if extra_zero != "0":
                            raise Exception("Failed to clean extra zeros from {}".format(bvecs_file))
                elif all(x == "0" for x in entries[-1:-min_sequence_length-1:-1]):
                    write(stdout, "Stripped trailing zero sequence from {}".format(bvecs_file))
                    while len(entries) > num_slices:
                        extra_zero = entries.pop(-1)
                        if extra_zero != "0":
                            raise Exception("Failed to clean extra zeros from {}".format(bvecs_file))

                if len(entries) > num_slices:
                    raise Exception('Failed to clean bvecs file {}. Since {} has {} slices, bvecs must have {} columns'.
                        format(bvecs_file, hardi_file, num_slices, num_slices))
                text += "0 " + " ".join(entries) + "\n" # restore leading zero
            f.seek(0)
            f.write(text)
            f.truncate()
            write(stdout, 'Generated bvecs file with values:\n{}'.format(text))

        # Compress DICOM inputs
        dicom_tmp_archive = strip_trailing_slash(dicom_tmp_dir) + '.tar.gz'
        smart_remove(dicom_tmp_archive)
        with tarfile.open(dicom_tmp_archive, mode='w:gz') as archive:
            archive.add(dicom_tmp_dir, recursive=True, arcname=basename(dicom_tmp_dir))
        smart_remove(dicom_tmp_dir)
        write(stdout, 'Compressed temporary DICOM files to {}'.format(dicom_tmp_archive))

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
