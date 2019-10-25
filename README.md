# TRACK TBI
Parallel EDI tractography workflow
<br></br>
<br></br>
### Setup

Requirements:
* Python 3.5+
* SLURM job scheduling on a multi-node system

<b>1\. Install NumPy and Parsl</b>

`pip3 install parsl numpy scipy`  
(`pip3 install parsl numpy scipy --user` for non-root systems)

<b>2\. Clone repository</b>

`git clone https://lc.llnl.gov/bitbucket/scm/tbi/tracktographyscripts.git`  
`cd tracktographyscripts`  

<b>3a\. Manually install dependencies</b>

Requirements:  
* FSL (https://fsl.fmrib.ox.ac.uk/fsl/fslwiki)
* Freesurfer (https://surfer.nmr.mgh.harvard.edu/fswiki)  
Optional:  
* Bedpostx GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)  
* Freesurfer GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)  
* CUDA 8.0, for Bedpostx GPU  
* CUDA 5.0, for Freesurfer GPU
* VTK 8.2 compiled with OSMesa and Python 3.5 wrappers, for image rendering  

**OR**

<b>3b\. Load a Singularity container</b>

Requirements:
* Singularity 3.0+ (https://www.sylabs.io/guides/3.0/user-guide/)

Building the container:  
i. Obtain root access (you can copy and run the image in a non-root system afterwards).  
ii. Place a Freesurfer `license.txt` in the repo directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).  
iii. `./container/build.sh`
<br></br>
<br></br>
### Launch
Specify parameters either in a config file or as command line arguments. See <b>s_run_all.py</b> for parameter details.

`./s_run_all.py <config_file>`

**OR**

`./s_run_all.py <arg1> <arg2> etc...`
<br></br>
<br></br>
### File Overview

```
TracktographyScripts/
+- container/
|  +- build.sh
|  +- Singularity               # Singularity build recipe
|
+- example_config.txt           # Example of how the config file should look like
|
+- license.txt                  # Not included, required to build Singularity container
|
+- lists/
|  +- example_subjects.txt      # Example of how the subject list should look like
|  +- list_edges_reduced.txt    # List of default edges to compute with Probtrackx and EDI (930 edges)
|  +- list_edges_all.txt        # List of all possible edges (6643 edges)
|  +- render_targets.txt        # List of NiFTI images to visualize with s4_render.py
|
+- README.md
|
+- s_run_all.py                 # Main script
|
+- subscripts/
   +- __init__.py
   +- maskseeds.py              # Helper functions for s2b_freesurfer.py
   +- run_vtk.py                # Helper script for s4_render.py
   +- s_debug.py                # Minimal debug step
   +- s1_dti_preproc.py
   +- s2a_bedpostx.py
   +- s2b_freesurfer.py
   +- s3_probtrackx.py
   +- s4_render.py
   +- utilities.py              # General utility functions
```
<br></br>
<br></br>
### Output Overview
The following are the most important output files. This list is not comprehensive.

```
<OUTPUT DIRECTORY>/
+- <SUBJECT NAME>/
   +- connectome_idxs.txt                    # Brain region indices for .mat connectome files
   +- connectome_oneway.txt                  # Oneway connectome in list form. Each edge has four columns:
                                             Column 1 is the source region
                                             Column 2 is the destination region
                                             Column 3 is number of fibers (NOF): the total count of successful streamlines between the two regions
                                             Column 4 is normalized NOF: the average density of successful streamlines the target region.
   +- connectome_twoway.txt                  # Twoway connectome in list form
   +- connectome_oneway_nof.mat              # Oneway NOF connectome in matrix form
   +- connectome_twoway_nof.mat              # Twoway NOF connectome in matrix form (should be symmetric)
   +- connectome_oneway_nof_normalized.mat   # Oneway normalized NOF connectome in matrix form
   +- connectome_twoway_nof_normalized.mat   # Twoway normalized NOF connectome in matrix form (should be symmetric)
   |
   +- EDI/
   |  +- EDImaps/
   |     +- FAtractsumsRaw.nii.gz     # NiFTI image of total streamline density
   |     +- FAtractsumsTwoway.nii.gz  # NiFTI image of edge density (EDI). See Payabvash et al. (2019) for details.
   |
   +- log/                            # Directory containing stdout and performance logs
   |
   +- render/                         # Directory containing NiFTI image renders from step s4_render
```
<br></br>
<br></br>
### Config Parameters/Command Line Arguments

| Required Parameter  | Description |
|---------------------|-------------|
| subjects_csv        | CSV file with input directories for each subject |
| output_dir          | The super-directory that will contain output directories for each subject. Avoid using a Lustre file system |
| scheduler_name      | Scheduler to be used for running jobs. Value is slurm at LLNL, cobalt at ANL |
| scheduler_bank      | Scheduler bank to charge for jobs |
| scheduler_partition | Scheduler partition to assign jobs |

<br></br>

| Optional Parameter    | Default                         | Description |
|-----------------------|---------------------------------|-------------|
| steps                 | s1 s2a s2b s3 s4                | Steps to run |
| gpu_steps             | s2a s2b s3                      | Steps to enable CUDA-enabled binaries |
| scheduler_options     |                                 | String to prepend to the submit script to the scheduler |
| gpu_options           |                                 | String to prepend to the submit blocks for GPU-enabled steps, such as 'module load cuda/8.0;' |
| worker_init           |                                 | String to run before starting a worker, such as ‘module load Anaconda; source activate env;’ |
| container_path        | container/image.simg            | Path to Singularity container image |
| unix_username         | [[current user]]                | Unix username for Parsl job requests  |
| unix_group            |                                 | Unix group to assign file permissions  |
| force                 | False                           | Force re-compute if checkpoints already exist  |
| gssapi                | False                           | Use Kerberos GSS-API authentication  |
| local_host_only       | True                            | Request all jobs on local machine, ignoring other hostnames |
| parsl_path            |                                 | Path to Parsl binaries, if not installed in /usr/bin or /usr/sbin |
| render_list           | lists/render_targets.txt        | Text file list of NIfTI outputs for s4_render (relative to each subject output directory)  |
| pbtx_sample_count     | 200                             | Number of streamlines per seed voxel in s3_probtrackx |
| fast_pbtx             | False                           | Use 1-to-N instead of N-to-N probtrackx script |
| pbtx_random_seed      | [[random number]]               | Random seed in s3_probtrackx |
| connectome_idx_list   | lists/connectome_idxs.txt       | Text file with pairs of volumes and connectome indices |
| histogram_bin_count   | 256                             | Number of bins in NiFTI image histograms |
| pbtx_edge_list        | lists/list_edges_reduced.txt    | Text file list of edges for steps s3_probtrackx |
| compress_pbtx_results | True                            | Compress probtrackx outputs to reduce inode and disk space usage |
| dynamic_walltime      | False                           | Request dynamically shortened walltimes, to gain priority on job queue |
| s1_job_time           | 00:15:00                        | Max time to finish s1 on 1 subject with 1 node, if dynamic_walltime is true |
| s2a_job_time          | 00:45:00                        | Max time to finish s2a on 1 subject with 1 node, if dynamic_walltime is true |
| s2b_job_time          | 10:00:00                        | Max time to finish s2b on 1 subject with 1 node, if dynamic_walltime is true |
| s3_job_time           | 23:59:00                        | Max time to finish s3 on 1 subject with 1 node, if dynamic_walltime is true |
| s4_job_time           | 00:15:00                        | Max time to finish s4 on 1 subject with 1 node, if dynamic_walltime is true |
| s1_cores_per_task     | 1                               | Number of cores to assign each task for step s1_dti_preproc |
| s2a_cores_per_task    | [[core count on head node]]     | Number of cores to assign each task for step s2a_bedpostx |
| s2b_cores_per_task    | [[core count on head node]]     | Number of cores to assign each task for step s2b_freesurfer |
| s3_cores_per_task     | 3                               | Number of cores to assign each task for step s3_probtrackx |
| s4_cores_per_task     | 1                               | Number of cores to assign each task for step s4_render |
| s1_hostname           |                                 | Hostname of machine to run step s1_dti_preproc, if local_host_only is false |
| s2a_hostname          |                                 | Hostname of machine to run step s2a_bedpostx, if local_host_only is false |
| s2b_hostname          |                                 | Hostname of machine to run step s2b_freesurfer, if local_host_only is false |
| s3_hostname           |                                 | Hostname of machine to run step s3_probtrackx, if local_host_only is false |
| s4_hostname           |                                 | Hostname of machine to run step s4_render, if local_host_only is false |
| s1_walltime           | 23:59:00                        | Walltime for step s1 |
| s2a_walltime          | 23:59:00                        | Walltime for step s2a |
| s2b_walltime          | 23:59:00                        | Walltime for step s2b |
| s3_walltime           | 23:59:00                        | Walltime for step s3 |
| s4_walltime           | 23:59:00                        | Walltime for step s4 |
| s1_nodes              | [[floor(0.2 * num_subjects)]]   | Node count for step s1 |
| s2a_nodes             | [[floor(1.0 * num_subjects)]]   | Node count for step s2a |
| s2b_nodes             | [[floor(1.0 * num_subjects)]]   | Node count for step s2b |
| s3_nodes              | [[floor(1.0 * num_subjects)]]   | Node count for step s3 |
| s4_nodes              | [[floor(0.1 * num_subjects)]]   | Node count for step s4 |
| s1_cores              | [[core count on head node]]     | Cores per node for step s1 |
| s2a_cores             | [[core count on head node]]     | Cores per node for step s2a |
| s2b_cores             | [[core count on head node]]     | Cores per node for step s2b |
| s3_cores              | [[core count on head node]]     | Cores per node for step s3 |
| s4_cores              | [[core count on head node]]     | Cores per node for step s4 |
