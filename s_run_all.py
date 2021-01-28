#!/usr/bin/env python3
import argparse,multiprocessing,parsl,getpass,socket,json,sys,re,glob
from parsl.app.app import python_app,bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiRunLauncher,SingleNodeLauncher,SimpleLauncher,SrunLauncher
from parsl.addresses import address_by_hostname,address_by_route
from parsl.providers import LocalProvider,SlurmProvider,CobaltProvider,GridEngineProvider
from parsl.channels import SSHInteractiveLoginChannel,LocalChannel,SSHChannel
from parsl.utils import get_all_checkpoints
from parsl.executors.ipp_controller import Controller
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,islink,isdir
from os import system,mkdir,remove,environ,makedirs,getcwd
from math import floor, ceil
from subscripts.s_debug import setup_debug
from subscripts.s1_dti_preproc import setup_s1
from subscripts.s2a_bedpostx import setup_s2a
from subscripts.s2b_freesurfer import setup_s2b
from subscripts.s3_probtrackx import setup_s3
from subscripts.s4_render import setup_s4

vCPUs_per_core = 2
head_node_cores = int(floor(multiprocessing.cpu_count() / vCPUs_per_core)) # We assume 2 vCPUs per core

class ArgsObject:
    def __init__(self, **entries):
        self.__dict__.update(entries)

if len(sys.argv) == 2 and sys.argv[1] not in ['-h', '--help']:
    config_json = sys.argv[1]
    with open(config_json) as f:
        raw_args = json.load(f)

    if not (bool('subject' in raw_args) ^ bool('subjects_json' in raw_args)):
        raise Exception('Config file {} must have exactly one of: subject or subjects_json'.format(config_file))

    missing_args = []
    for required_arg in ['output_dir', 'scheduler_name']:
        if required_arg not in raw_args:
            missing_args.append(required_arg)
    if missing_args:
        raise Exception('Config file {} is missing required arguments: {}'.format(config_json, missing_args))
    args = ArgsObject(**raw_args)
else:
    parser = argparse.ArgumentParser(description='Generate connectome and edge density images',
        usage="""%(prog)s --subject SUBJECT_NAME --scheduler_name SCHEDULER --output_dir OUT_DIR

<< OR >>

usage: %(prog)s --subjects_json SUBJECTS_JSON --scheduler_name SCHEDULER --output_dir OUT_DIR

<< OR >>

usage: %(prog)s [config_json]

(see optional arguments with --help)""")
    
    ### ==================
    ### Required arguments
    ### ==================
    subjects_group = parser.add_mutually_exclusive_group(required=True)
    subjects_group.add_argument('--subject', help='Name of single subject.')
    subjects_group.add_argument('--subjects_json', help='JSON file with input directories for each subject.')
    parser.add_argument('--output_dir', help='The directory that will contain a BIDS-compliant dataset with all subjects.', required=True)
    parser.add_argument('--scheduler_name', help='Scheduler to be used for running jobs. Value is "slurm" for LLNL, "cobalt" for ANL, and "grid_engine" for UCSF.', required=True, choices=['slurm', 'cobalt', 'grid_engine'])
    
    # Required for Slurm and Cobalt
    parser.add_argument('--scheduler_partition', help='Scheduler partition to assign jobs. Required for slurm and cobalt.')
    parser.add_argument('--scheduler_bank', help='Scheduler scheduler_bank to charge for jobs. Required for slurm and cobalt.')

    ### ==================
    ### Optional arguments
    ### ==================

    parser.add_argument('--scheduler_options', help='String to prepend to the submit script to the scheduler')
    parser.add_argument('--gpu_options', help="String to prepend to the submit blocks for GPU-enabled steps, such as 'module load cuda/8.0;'")
    parser.add_argument('--worker_init', help="String to run before starting a worker, such as ‘module load Anaconda; source activate env;’")
    parser.add_argument('--steps', type=str.lower, help='Steps to run with this workflow', nargs='+')
    parser.add_argument('--gpu_steps', type=str.lower, help='Steps to run using CUDA-enabled binaries', nargs='+')
    parser.add_argument('--pbtx_edge_list', help='Text file list of edges processed by s3_probtrackx')
    parser.add_argument('--unix_username', help='Unix username for Parsl job requests')
    parser.add_argument('--unix_group', help='Unix group to assign file permissions')
    parser.add_argument('--container_path', help='Path to Singularity container image')
    parser.add_argument('--container_cwd', help='Path to current working directory for Singularity.')
    parser.add_argument('--parsl_path', help='Path to Parsl binaries, if not installed in /usr/bin or /usr/sbin')
    parser.add_argument('--render_list', help='Text file list of NIfTI outputs for s4_render (relative to each subject output directory).')
    parser.add_argument('--gssapi', help='Use Kerberos GSS-API authentication.', action='store_true')
    parser.add_argument('--force', help='Force re-compute', action='store_true')
    parser.add_argument('--force_params', help='Force re-compute if new parameters do not match previous run', action='store_false')
    parser.add_argument('--local_host_only', help='Request all jobs on same host as strategy node, ignoring other hostnames.', action='store_false')
    parser.add_argument('--pbtx_sample_count', help='Number of streamlines in s3_probtrackx')
    parser.add_argument('--pbtx_random_seed', help='Random seed in s3_probtrackx')
    parser.add_argument('--pbtx_edge_chunk_size', help='Edges per task in s3_probtrackx. Set high if number of jobs causes log output to crash.')
    parser.add_argument('--pbtx_max_memory', help='Usable memory per node (in GB) for s3_probtrackx without GPU. Default value of 0 indicates unlimited memory bound. If you are running into memory limitations, this value should be somewhere between 50-90 percent of a single node\'s available RAM.')
    parser.add_argument('--pbtx_max_gpu_memory', help='Usable video memory per node (in GB) for s3_probtrackx with GPU. Default value of 0 indicates unlimited memory bound. If you are running into memory limitations, this value should be somewhere between 30-70 percent of a single node\'s available VRAM.')
    parser.add_argument('--connectome_idx_list', help='Text file with pairs of volumes and connectome indices')
    parser.add_argument('--histogram_bin_count', help='Number of bins in NiFTI image histograms')
    parser.add_argument('--compress_pbtx_results', help='Compress probtrackx outputs to reduce inode and disk space usage', action='store_false')
    parser.add_argument('--retries', help='Number of times to retry failed tasks')

    # Site-specific machine settings
    parser.add_argument('--s1_hostname', help='Hostname of machine to run step s1_dti_preproc')
    parser.add_argument('--s2a_hostname', help='Hostname of machine to run step s2a_bedpostx')
    parser.add_argument('--s2b_hostname', help='Hostname of machine to run step s2b_freesurfer')
    parser.add_argument('--s3_hostname', help='Hostname of machine to run step s3_probtrackx')
    parser.add_argument('--s4_hostname', help='Hostname of machine to run step s4_render')
    parser.add_argument('--s1_cores_per_task', help='Number of cores to assign each task for step s1_dti_preproc')
    parser.add_argument('--s2a_cores_per_task', help='Number of cores to assign each task for step s2a_bedpostx')
    parser.add_argument('--s2b_cores_per_task', help='Number of cores to assign each task for step s2b_freesurfer')
    parser.add_argument('--s3_cores_per_task', help='Number of cores to assign each task for step s3_probtrackx')
    parser.add_argument('--s4_cores_per_task', help='Number of cores to assign each task for step s4_render')
    parser.add_argument('--s1_walltime', help='Walltime for step s1.')
    parser.add_argument('--s2a_walltime', help='Walltime for step s2a.')
    parser.add_argument('--s2b_walltime', help='Walltime for step s2b.')
    parser.add_argument('--s3_walltime', help='Walltime for step s3.')
    parser.add_argument('--s4_walltime', help='Walltime for step s4.')

    # Dynamic walltime (disabled by default)
    parser.add_argument('--dynamic_walltime', help='Request dynamically shortened walltimes, to gain priority on job queue', action='store_true')
    parser.add_argument('--s1_job_time', help='If using dynamic walltime, duration of s1 on a single subject with a single node')
    parser.add_argument('--s2a_job_time', help='If using dynamic walltime, duration of s2a on a single subject with a single node')
    parser.add_argument('--s2b_job_time', help='If using dynamic walltime, duration of s2b on a single subject with a single node')
    parser.add_argument('--s3_job_time', help='If using dynamic walltime, duration of s3 on a single subject with a single node')
    parser.add_argument('--s4_job_time', help='If using dynamic walltime, duration of s4 on a single subject with a single node')

    # Override auto-generated machine settings
    parser.add_argument('--s1_nodes', help='Override recommended node count for step s1.')
    parser.add_argument('--s2a_nodes', help='Override recommended node count for step s2a.')
    parser.add_argument('--s2b_nodes', help='Override recommended node count for step s2b.')
    parser.add_argument('--s3_nodes', help='Override recommended node count for step s3.')
    parser.add_argument('--s4_nodes', help='Override recommended node count for step s4.')
    parser.add_argument('--s1_cores', help='Override core count for node running step s1.')
    parser.add_argument('--s2a_cores', help='Override core count for node running step s2a.')
    parser.add_argument('--s2b_cores', help='Override core count for node running step s2b.')
    parser.add_argument('--s3_cores', help='Override core count for node running step s3.')
    parser.add_argument('--s4_cores', help='Override core count for node running step s4.')

    # BIDS specification settings
    parser.add_argument('--bids_json', help='Description file dataset_description.json, as specified at https://bids-specification.readthedocs.io/en/stable/03-modality-agnostic-files.html')
    parser.add_argument('--bids_readme', help='Free form text file describing the dataset in more detail.')
    parser.add_argument('--bids_session_name', help='Name for the session timepoint (e.g. 2weeks).')

    args = parser.parse_args()

############################
# Defaults for Optional Args
############################

pending_args = args.__dict__.copy()
parse_default('steps', "s1 s2a s2b s3 s4", args, pending_args)
parse_default('gpu_steps', "s2a", args, pending_args)
parse_default('pbtx_edge_list', join("lists","list_edges_reduced.txt"), args, pending_args)
parse_default('scheduler_name', "slurm", args, pending_args)
parse_default('scheduler_partition', "", args, pending_args)
parse_default('scheduler_bank', "", args, pending_args)
parse_default('scheduler_options', "", args, pending_args)
parse_default('worker_init', "", args, pending_args)
parse_default('gpu_options', "", args, pending_args)
parse_default('container_path', "container/image.simg", args, pending_args)
parse_default('container_cwd', "", args, pending_args)
parse_default('unix_username', getpass.getuser(), args, pending_args)
parse_default('unix_group', None, args, pending_args)
parse_default('force', False, args, pending_args)
parse_default('force_params', True, args, pending_args)
parse_default('gssapi', False, args, pending_args)
parse_default('local_host_only', True, args, pending_args)
parse_default('compress_pbtx_results', True, args, pending_args)
parse_default('parsl_path', None, args, pending_args)
parse_default('render_list', "lists/render_targets.txt", args, pending_args)
parse_default('pbtx_sample_count', 200, args, pending_args)
parse_default('pbtx_random_seed', None, args, pending_args)
parse_default('pbtx_edge_chunk_size', 16, args, pending_args)
parse_default('pbtx_max_memory', 0, args, pending_args)
parse_default('pbtx_max_gpu_memory', 0, args, pending_args)
parse_default('connectome_idx_list', "lists/connectome_idxs.txt", args, pending_args)
parse_default('histogram_bin_count', 256, args, pending_args)
parse_default('retries', 5, args, pending_args)
parse_default('s1_cores_per_task', 1, args, pending_args)
parse_default('s2a_cores_per_task', head_node_cores, args, pending_args)
parse_default('s2b_cores_per_task', head_node_cores, args, pending_args)
parse_default('s3_cores_per_task', 1, args, pending_args)
parse_default('s4_cores_per_task', 1, args, pending_args)
parse_default('s1_walltime', "23:59:00", args, pending_args)
parse_default('s2a_walltime', "23:59:00", args, pending_args)
parse_default('s2b_walltime', "23:59:00", args, pending_args)
parse_default('s3_walltime', "23:59:00", args, pending_args)
parse_default('s4_walltime', "23:59:00", args, pending_args)
parse_default('dynamic_walltime', False, args, pending_args)
parse_default('s1_job_time', "00:15:00", args, pending_args)
parse_default('s2a_job_time', "00:45:00", args, pending_args)
parse_default('s2b_job_time', "10:00:00", args, pending_args)
parse_default('s3_job_time', "23:59:00", args, pending_args)
parse_default('s4_job_time', "00:45:00", args, pending_args)
parse_default('bids_json', "examples/dataset_description.json", args, pending_args)
parse_default('bids_readme', "", args, pending_args)
parse_default('bids_session_name', "", args, pending_args)

for step in ['s1','s2a','s2b','s3','s4']:
    parse_default(step + '_hostname', None, args, pending_args)
    parse_default(step + '_nodes', None, args, pending_args)
    parse_default(step + '_cores', None, args, pending_args)

pending_args.pop('subject', None)
pending_args.pop('subjects_json', None)
pending_args.pop('output_dir', None)
if len(pending_args) != 0:
    raise Exception("Invalid arguments: {}".format(pending_args))

############################
# Steps
############################

print("\n===================================================")
print("Setup")
print("---------------------------------------------------")
steps = args.steps if args.steps else []
gpu_steps = args.gpu_steps if args.gpu_steps else []
if isinstance(args.steps, str):
    steps = [x.strip() for x in args.steps.split(' ') if x]
if isinstance(args.gpu_steps, str):
    gpu_steps = [x.strip() for x in args.gpu_steps.split(' ') if x]
valid_steps = ['debug','s1','s2a','s2b','s3','s4']
steps = [x for x in steps if x in valid_steps]
gpu_steps = [x for x in gpu_steps if x in valid_steps]
if len(steps) != len(set(steps)):
    raise Exception("Argument \"steps\" has duplicate values")
if len(gpu_steps) != len(set(gpu_steps)):
    raise Exception("Argument \"gpu_steps\" has duplicate values")
if hasattr(args, 'subject') and args.subject:
    if 's1' in steps:
        raise Exception("Step s1 must be run with \"--subjects_json\", not \"--subject\". This is because you need to specify NiFTI/DICOM input directories.")
    
    single_subject_path = join(abspath(args.output_dir), 'derivatives', '**', get_bids_subject_name(args.subject))
    if len(glob(single_subject_path, recursive=True)) == 0:
        raise Exception("No subject matches {}\nNote: subject directory must be named according to BIDS format.".format(single_subject_path))

step_setup_functions = {
    'debug': setup_debug,
    's1': setup_s1,
    's2a': setup_s2a,
    's2b': setup_s2b,
    's3': setup_s3,
    's4': setup_s4,
}
prereqs = {
    'debug': [],
    's1': [],
    's2a': ['s1'],
    's2b': ['s1'],
    's3': ['s1','s2a','s2b'],
    's4': ['s1','s2a','s2b','s3'],
}
# Print warning if prerequisite steps are not running
for step in steps:
    for prereq in prereqs[step]:
        if prereq not in steps:
            print("Warning: step {} has prerequisite steps {}. You are only running steps {}.".format(step, prereqs[step], steps))
            break

############################
# Inputs
############################

output_dir = abspath(args.output_dir)
sourcedata_dir = join(output_dir, 'sourcedata')
rawdata_dir = join(output_dir, 'rawdata')
derivatives_dir = join(output_dir, 'derivatives')
container = abspath(args.container_path) if args.container_path else None
smart_mkdir(sourcedata_dir)
smart_mkdir(rawdata_dir)
smart_mkdir(derivatives_dir)
smart_mkdir(join(derivatives_dir, 'global_timing'))
derivatives_dir_tmp = join(derivatives_dir, 'tmp')
smart_remove(derivatives_dir_tmp)
smart_mkdir(derivatives_dir_tmp)
global_timing_log = join(derivatives_dir, 'global_timing', 'timing.csv')
if not exists(global_timing_log):
    write(global_timing_log, "subject,step,ideal_walltime,actual_walltime,total_core_time,use_gpu")
# if islink(join(derivatives_dir,"fsaverage")):
    # run("unlink {}".format(join(derivatives_dir,"fsaverage")))
pbtx_edge_list = abspath(args.pbtx_edge_list)
render_list = abspath(args.render_list)
connectome_idx_list = abspath(args.connectome_idx_list)
smart_copy(args.bids_json, join(rawdata_dir, "dataset_description.json"))
if args.bids_readme:
    smart_copy(args.bids_readme, join(rawdata_dir, "README"))

subject_dict = {}

json_data = {}
if hasattr(args, 'subjects_json') and args.subjects_json is not None:
    with open(args.subjects_json, newline='') as json_file:
        json_data = json.load(json_file)
else:
    json_data = {args.subject:{}}

for sname in json_data:
    sname = sname.replace('sub-', '') # make naming consistent
    subject_name = get_bids_subject_name(sname)
    if args.bids_session_name:
        session_name = args.bids_session_name.replace('ses-', '')
        session_name = 'ses-{}'.format(regex.sub('', session_name))
        sdir = join(derivatives_dir, subject_name, session_name)
        bids_dicom_dir = join(sourcedata_dir, subject_name, session_name)
        bids_nifti_dir = join(rawdata_dir, subject_name, session_name)
    else:
        session_name = ""
        sdir = join(derivatives_dir, subject_name)
        bids_dicom_dir = join(sourcedata_dir, subject_name)
        bids_nifti_dir = join(rawdata_dir, subject_name)
    log_dir = join(sdir,'log')
    smart_mkdir(log_dir)
    smart_mkdir(sdir)
    smart_mkdir(bids_dicom_dir)
    smart_mkdir(bids_nifti_dir)

    T1_dicom_dir = json_data[sname]['T1_dicom_dir'] if 'T1_dicom_dir' in json_data[sname] else ''
    DTI_dicom_dir = json_data[sname]['DTI_dicom_dir'] if 'DTI_dicom_dir' in json_data[sname] else ''
    extra_b0_dirs = json_data[sname]['extra_b0_dirs'] if 'extra_b0_dirs' in json_data[sname] else []
    nifti_dir = json_data[sname]['nifti_dir'] if 'nifti_dir' in json_data[sname] else ''

    if 's1' in steps:
        # Make sure input files exist for each subject
        if not T1_dicom_dir or not isdir(T1_dicom_dir) or not DTI_dicom_dir or not isdir(DTI_dicom_dir):
            if not nifti_dir or not isdir(nifti_dir):
                print('Invalid subject {} in {}\nWhen running s1_dti_preproc, you must specify T1_dicom_dir and DTI_dicom_dir.'.format(sname, args.subjects_json) +
                    ' Or specify nifti_dir with bvecs, bvals, hardi.nii.gz, and anat.nii.gz already in place.')
                continue

            bvecs = join(nifti_dir, "bvecs")
            bvals = join(nifti_dir, "bvals")
            hardi = join(nifti_dir, "hardi.nii.gz")
            anat = join(nifti_dir, "anat.nii.gz")

            # If input files not found, try to substitute alternative files
            data = join(nifti_dir, "data.nii.gz")
            nii_data = join(nifti_dir, "data.nii")
            nii_hardi = join(nifti_dir, "hardi.nii")
            if not exists(hardi):
                if exists(nii_hardi):
                    smart_copy(compress_file(nii_hardi), hardi)
                elif exists(data):
                    smart_copy(data, hardi)
                elif exists(nii_data):
                    smart_copy(compress_file(nii_data), hardi)
            T1 = join(nifti_dir, "T1.nii.gz")
            nii_T1 = join(nifti_dir, "T1.nii")
            nii_anat = join(nifti_dir, "anat.nii")
            if not exists(anat):
                if exists(nii_anat):
                    smart_copy(compress_file(nii_anat), anat)
                elif exists(T1):
                    smart_copy(T1, anat)
                elif exists(nii_T1):
                    smart_copy(compress_file(nii_T1), anat)

            if not exist_all([bvecs, bvals, hardi, anat]):
                print('Invalid subject {} in {}\nSince T1_dicom_dir and DTI_dicom_dir are not specified, you must specify nifti_dir with '.format(sname, args.subjects_json) +
                        'bvecs, bvals, hardi.nii.gz, and anat.nii.gz.'.format(sname, args.subjects_json))
                continue
    
    subject = {}
    for step in steps:
        params = {
            # Preprocessing parameters
            'T1_dicom_dir': T1_dicom_dir,
            'DTI_dicom_dir': DTI_dicom_dir,
            'extra_b0_dirs': extra_b0_dirs,
            'src_nifti_dir': nifti_dir,

            # BIDS parameters
            'output_dir': output_dir,
            'sourcedata_dir': sourcedata_dir,
            'rawdata_dir': rawdata_dir,
            'derivatives_dir': derivatives_dir,
            'bids_dicom_dir': bids_dicom_dir,
            'bids_nifti_dir': bids_nifti_dir,
            'subject_name': subject_name,
            'session_name': session_name,

            # General parameters
            'sname': sname,
            'sdir': sdir,
            'container': container,
            'pbtx_edge_list': pbtx_edge_list,
            'group': args.unix_group,
            'global_timing_log': global_timing_log,
            'use_gpu': step in gpu_steps,
            'step': step,
            'render_list': render_list,
            'connectome_idx_list': connectome_idx_list,
            'pbtx_sample_count': int(args.pbtx_sample_count),
            'pbtx_random_seed': args.pbtx_random_seed,
            'pbtx_edge_chunk_size': int(args.pbtx_edge_chunk_size),
            'pbtx_max_memory': args.pbtx_max_memory,
            'pbtx_max_gpu_memory': args.pbtx_max_gpu_memory,
            'histogram_bin_count': int(args.histogram_bin_count),
            'compress_pbtx_results': args.compress_pbtx_results,
        }
        stdout_template = join(log_dir, "{}.stdout".format(step))
        new_stdout, prev_stdout, idx = get_log_path(stdout_template)
        params_log = join(log_dir, step + "_params.txt")
        if exists(params_log) and not args.force:
            with open(params_log) as f:
                old_params = json.load(f)
                for k in params:
                    if args.force_params and (k not in old_params or str(params[k]) != str(old_params[k])):
                        break
                else:
                    if is_log_complete(prev_stdout):
                        print("Skipping step \"{}\" for subject {} after finding completed log at {}. Use --force to rerun.".format(step, sname, prev_stdout))
                        continue
                    else:
                        if exists(prev_stdout):
                            new_stdout = prev_stdout
                            idx -= 1
        timing_log = join(log_dir, "{}_{:02d}_timing.txt".format(step, idx))
        smart_remove(params_log)
        smart_remove(timing_log)
        with open(params_log, 'w') as f:
            json.dump(params, f)
        params['stdout'] = new_stdout
        params['timing_log'] = timing_log

        subject[step] = params
    subject_dict[sname] = subject
    running_steps = list(subject.keys())
    if len(running_steps) > 0:
        print("Running subject {} with steps {}".format(sname, running_steps))
    else:
        print("Not running any steps for subject {}".format(sname))

num_subjects = {
    'debug': 0,
    's1': 0,
    's2a': 0,
    's2b': 0,
    's3': 0,
    's4': 0,
}
for sname in subject_dict:
    for step in subject_dict[sname]:
        num_subjects[step] += 1
total_num_steps = sum(num_subjects.values())
if total_num_steps == 0:
    raise Exception("Not running any steps on any subjects")
elif 's3' in steps:
    subject_samples = len(subject_dict) * int(args.pbtx_sample_count)
    if subject_samples > 10000:
        raise Exception("Do not run probtrackx tractography with more than 10000 subjects * pbtx_samples. Frequent instability on Quartz.")
else:
    print("In total, running {} steps across {} subjects".format(total_num_steps, len(subject_dict)))

############################
# Node Settings
############################

def get_walltime(_num_subjects, job_time_string, node_count):
    job_time = get_time_seconds(job_time_string)
    return get_time_string(clamp((_num_subjects * job_time) / node_count, 7200, 86399)) # clamp between 2 and 24 hours

# Set recommended or overriden node counts
node_counts = {
    'debug': 1,
    's1': min(max(floor(0.1 * num_subjects['s1']), 1), 8) if args.s1_nodes is None else int(args.s1_nodes),
    's2a': min(max(floor(1.0 * num_subjects['s2a']), 1), 24) if args.s2a_nodes is None else int(args.s2a_nodes),
    's2b': min(max(floor(1.0 * num_subjects['s2b']), 1), 24) if args.s2b_nodes is None else int(args.s2b_nodes),
    's3': min(max(floor(1.0 * num_subjects['s3']), 1), 24) if args.s3_nodes is None else int(args.s3_nodes),
    's4': min(max(floor(0.1 * num_subjects['s4']), 1), 8) if args.s4_nodes is None else int(args.s4_nodes),
}
job_times = {
    's1': args.s1_job_time,
    's2a': args.s2a_job_time,
    's2b': args.s2b_job_time,
    's3': args.s3_job_time,
    's4': args.s4_job_time,
}
walltimes = {
    'debug': "00:05:00",
    's1': get_walltime(num_subjects['s1'], job_times['s1'], node_counts['s1']) if args.dynamic_walltime else args.s1_walltime,
    's2a': get_walltime(num_subjects['s2a'], job_times['s2a'], node_counts['s2a']) if args.dynamic_walltime else args.s2a_walltime,
    's2b': get_walltime(num_subjects['s2b'], job_times['s2b'], node_counts['s2b']) if args.dynamic_walltime else args.s2b_walltime,
    's3': get_walltime(num_subjects['s3'], job_times['s3'], node_counts['s3']) if args.dynamic_walltime else args.s3_walltime,
    's4': get_walltime(num_subjects['s4'], job_times['s4'], node_counts['s4']) if args.dynamic_walltime else args.s4_walltime,
}
hostnames = {
    'debug': args.s1_hostname,
    's1': args.s1_hostname,
    's2a': args.s2a_hostname,
    's2b': args.s2b_hostname,
    's3': args.s3_hostname,
    's4': args.s4_hostname,
}
cores_per_node = {
    'debug': head_node_cores,
    's1': head_node_cores if args.s1_cores is None else int(args.s1_cores),
    's2a': head_node_cores if args.s2a_cores is None else int(args.s2a_cores),
    's2b': head_node_cores if args.s2b_cores is None else int(args.s2b_cores),
    's3': head_node_cores if args.s3_cores is None else int(args.s3_cores),
    's4': head_node_cores if args.s4_cores is None else int(args.s4_cores),
}
cores_per_task = {
    'debug': 1,
    's1': args.s1_cores_per_task,
    's2a': args.s2a_cores_per_task,
    's2b': args.s2b_cores_per_task,
    's3': args.s3_cores_per_task, # setting this too low can lead to probtrackx exceeding local memory and crashing
    's4': args.s4_cores_per_task,
}

############################
# Parsl
############################

if not args.local_host_only:
    if not exists('/usr/bin/ipengine') and not exists('/usr/sbin/ipengine') and not exists(join(args.parsl_path, 'ipengine')):
        raise Exception("Could not find Parsl system install containing executable \"ipengine\". Please set --parsl_path to its install location.")

# Cobalt and Slurm reqire scheduler_bank and scheduler_partition
if args.scheduler_name in ['slurm', 'cobalt']:
    if not args.scheduler_bank:
        raise Exception("Scheduler {} requires config parameter scheduler_bank".format(args.scheduler_name))
    if not args.scheduler_partition:
        raise Exception("Scheduler {} requires config parameter scheduler_partition".format(args.scheduler_name))

base_options = ""
if args.scheduler_name in ['slurm']:
    base_options += "#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.scheduler_bank)
base_options += str(args.scheduler_options) + '\n'
if args.parsl_path is not None:
    base_options += "PATH=\"{}:$PATH\"\nexport PATH\n".format(args.parsl_path)

worker_init = args.worker_init
worker_init += "\nexport PYTHONPATH=$PYTHONPATH:{}".format(getcwd())

executors = []
for step in steps:
    node_count = int(node_counts[step])
    print("Requesting {} nodes for step \"{}\"".format(node_count, step))
    options = base_options
    if step in gpu_steps:
        options += str(args.gpu_options) + '\n'
    if args.local_host_only:
        channel = LocalChannel()
    else:
        if hostnames[step] is None:
            raise Exception('To run step {} on a remote host, please set the argument --{}_hostname'.format(step))
        channel = SSHChannel(
                hostname=hostnames[step],
                username=args.unix_username,
                gssapi_auth=args.gssapi,
                )
    if args.scheduler_name == 'slurm':
        executors.append(HighThroughputExecutor(
                    label=step,
                    worker_debug=True,
                    address=address_by_hostname(),
                    cores_per_worker=vCPUs_per_core*int(cores_per_task[step]),
                    provider=SlurmProvider(
                        args.scheduler_partition,
                        channel=channel,
                        launcher=SrunLauncher(),
                        nodes_per_block=node_count,
                        worker_init=worker_init,
                        init_blocks=1,
                        max_blocks=1,
                        walltime=walltimes[step],
                        scheduler_options=options,
                        move_files=False,
                    ),
                )
            )
    elif args.scheduler_name == 'grid_engine':
        executors.append(HighThroughputExecutor(
                    label=step,
                    worker_debug=True,
                    address=address_by_hostname(),
                    provider=GridEngineProvider(
                        channel=channel,
                        launcher=SingleNodeLauncher(),
                        nodes_per_block=node_count,
                        worker_init=worker_init,
                        init_blocks=1,
                        max_blocks=1,
                        walltime=walltimes[step],
                        scheduler_options=options,
                    ),
                )
            )
    elif args.scheduler_name == 'cobalt':
        executors.append(HighThroughputExecutor(
                    label=step,
                    worker_debug=True,
                    address=address_by_hostname(),
                    provider=CobaltProvider(
                        queue=args.scheduler_partition,
                        account=args.scheduler_bank, # project name to submit the job
                        # account='CSC249ADOA01',
                        channel=channel,
                        launcher=SimpleLauncher(),
                        scheduler_options=options,  # string to prepend to #COBALT blocks in the submit script to the scheduler
                        worker_init=worker_init, # command to run before starting a worker, such as 'source activate env'
                        # worker_init='source /home/madduri/setup_cooley_env.sh',
                        init_blocks=1,
                        max_blocks=1,
                        nodes_per_block=node_count,
                        walltime=walltimes[step],
                    ),
                )
            )
    else:
        raise Exception('Invalid scheduler_name {}. Valid schedulers are slurm, grid_engine, and cobalt.'.format(args.scheduler_name))
print("===================================================\n")

config = Config(executors=executors)
config.retries = int(args.retries)
config.checkpoint_mode = 'task_exit'
if not args.force:
    config.checkpoint_files = get_all_checkpoints()
parsl.set_stream_logger()
parsl.load(config)

############################
# Outputs
############################

all_jobs = []
for sname in subject_dict:
    subject_jobs = {} # Store jobs in previous steps to use as inputs
    for step in subject_dict[sname]:
        params = subject_dict[sname][step]
        params['cores_per_task'] = int(cores_per_task[step])
        inputs = []
        actual_prereqs = []
        for prereq in prereqs[step]:
            if prereq in subject_jobs:
                inputs.append(subject_jobs[prereq])
                actual_prereqs.append(prereq)
        print("Starting step {} after prereq steps {}".format(step, actual_prereqs))
        job_future = step_setup_functions[step](params, inputs)
        subject_jobs[step] = job_future
        all_jobs.append((params, job_future))
        
for job in all_jobs:
    params = job[0]
    job_future = job[1]
    if job_future is None:
        continue
    try:
        job_future.result()
        print("Finished processing step \"{}\" for subject {}".format(params['step'], params['sname']))
    except:
        print("Error: failed to process step \"{}\" for subject {}".format(params['step'], params['sname']))
    
