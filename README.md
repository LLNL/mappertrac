# TRACK TBI

Parallel EDI tractography workflow

Requirements:
* Python 3.5+
* Parsl (http://parsl-project.org/)
* SLURM job scheduling on a multi-node system

It can be run two ways:

### Using local libraries
Requirements:
* FSL (https://fsl.fmrib.ox.ac.uk/fsl/fslwiki)
* Freesurfer (https://surfer.nmr.mgh.harvard.edu/fswiki)

Optional:
* Bedpostx GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)
* Freesurfer GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)
* CUDA 8.0, for Bedpostx GPU
* CUDA 5.0, for Freesurfer GPU

Running the tractography script
1. Create a subject list, by writing input directories to a text file. Each input directory must contain hardi.nii.gz, anat.nii.gz, bvals, and bvecs.
2. Choose an output directory. Each subject may consume large amounts of disk space (>5 GB per subject).
3. Setup environment variables. From the repo directory, run `source local_env.sh <FSL dir> <Freesurfer dir> <CUDA 8 lib dir (optional)> <CUDA 5 lib dir (optional)>`
4. From the repo directory, run `./s_all_parsl.py <subject_list> <output_dir>`

**OR**

### Using a Singularity container
Requirements:
* Singlarity 2.5.2-2.6.0 (https://www.sylabs.io/guides/2.6/user-guide)
* Nvidia Tesla GPU hardware

Building the container

1. Make sure you have root access to the building system (you can copy the image to a non-root system afterwards).
2. Place a Freesurfer `license.txt` in the repo directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).
3. From the repo directory, run `./container/build.sh`

Running the tractography script

1. Create a subject list (see previous).
2. Choose an output directory (see previous).
3. From the repo directory, run `./s_all_parsl.py <subject_list> <output_dir> --container=container/image.simg`

### File Overview

```
TracktographyScripts/
+- local_env.sh                 # source local_env.sh <FSL dir> <Freesurfer dir> <CUDA 8 lib dir> <CUDA 5 lib dir (optional)>
|                                 Load required libraries from local installations
|
+- container/
|  +- build.sh                  # ./container/build.sh
|  |                              Build a compressed Singularity image with required libraries
|  |
|  +- Singularity               # Singularity build recipe
|
+- license.txt                  # Not included, required to use Freesurfer in container
|
+- lists/
|  +- listEdgesEDI.txt          # List of default edges to compute with Probtrackx and EDI (930 edges)
|  +- listEdgesEDIAll.txt       # List of all possible edges (6643 edges)
|  +- subcorticalIndex.txt      # List of regions post-processed in Freesurfer step
|  +- subjects_example.txt      # Example of how the subject list should look like
+- README.md
|
+- s_all_parsl.py               # ./s_all_parsl.py <subject_list> <output_dir>
|                                 Main script
|
+- subscripts/
   +- __init__.py
   +- config.py                 # Holds pre-process state, e.g. Parsl executor labels
   +- maskseeds.py              # Helper functions for s2b_freesurfer.py
   +- s1_dti_preproc.py
   +- s2a_bedpostx.py
   +- s2b_freesurfer.py
   +- s3_probtrackx.py
   +- s4_edi.py
   +- utilities.py              # General utility functions
```
