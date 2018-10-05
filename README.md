# TRACK TBI

### Running Scripts
Requires a local install of: 
* FSL (https://fsl.fmrib.ox.ac.uk/fsl/fslwiki)
* Freesurfer (https://surfer.nmr.mgh.harvard.edu/fswiki)
* Parsl (http://parsl-project.org/)
* Python 3.5+

With optional libraries:
* Bedpostx GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)
* CUDA 6.5-9.2, for Bedpostx GPU
* CUDA 5.0, for Freesurfer GPU

**OR**

Support for Singlarity containers, version 2.5+ (https://www.sylabs.io/guides/2.5/user-guide)

```
IMPORTANT: Either way, always call scripts from the repo's base directory
```

### Building the Singularity Container ###

Though Singularity can run with limited privileges, building a container requires root access. 

From the base directory, simply run "./container/build.sh". This process requires internet connection and 10 GB free disk space. It may take several hours, depending on connection speed.

If you do not have root access (e.g. using a cluster), then clone this repo on a system where you do. After building, copy "container/image.simg" into the repo on the desired system.

Make sure to place a Freesurfer license in the base directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).

### Local Workflow

```
source configLocal.sh <FSL dir> <Freesurfer dir> <CUDA 8 lib64 dir> <CUDA 5 lib64 dir>
./scheduleLocal/genScripts.py <path to subject dir (should contain hardi.nii.gz, bvals, bvecs)> <path to output dir>
msub scheduleLocal/s1_dtiPreproc.local.qsub
```
After it finishes, repeat msub with s2a and s2b (independent, so can be done in simultaneously). After both of those finish, msub s3. After that finishes, msub s4.
TODO: replace workflow with single Parsl script

### Singularity Workflow

```
./scheduleContainer/genScripts.py <path to subject dir (should contain hardi.nii.gz, bvals, bvecs)> <path to output dir>
msub scheduleContainer/s1_dtiPreproc.qsub
```
Just like before, run the subsequent steps after the previous ones finish.

### File Overview

```
TracktographyScripts/
+- configLocal.sh               # source configLocal.sh <FSL dir> <Freesurfer dir> <CUDA 8 lib dir (optional)> <CUDA 5 lib dir (optional)> 
|                                 IMPORTANT: Run this before using local install
+- container/
|  +- build.sh                  # ./container/build.sh
|  |                              Build a compressed Singularity image with required libraries
|  +- internal/
|  |  +- fslinstaller.py        # Custom FSL install script (used while building, do not change)
|  |  +- run.py                 # Container runscript (used while building, do not change)
|  |
|  +- image.simg                # Not included
|  |                              Singularity container with FSL & Freesurfer
|  |
|  +- run.sh                    # ./container/run.sh <command>
|  |                              Run command in container
|  |
|  +- runGPU.sh                 # ./container/runGPU.sh <command>
|  |                              Run GPU-enabled command in container, requires Nvidia Tesla GPU
|  |
|  +- shell.sh                  # ./container/shell.sh
|  |                              Open shell in container
|  |
|  +- Singularity               # Singularity build recipe
|
+- license.txt                  # Not included, required to use Freesurfer in container
|                                 Download at https://surfer.nmr.mgh.harvard.edu/fswiki/License
+- lists/
|  +- listEdgesEDI.txt          # See s4_consensusEDI.py
|  +- subcorticalIndex.txt      # See s2b_freesurfer.py
+- README.md
+- s1_dtiPreproc.py             # ./s1_dtiPreproc.py <input dir> <output dir>
|                                 Setup and image correction (~5 minutes with 36 cores)
|
+- s2a_bedpostx.py              # ./s2a_bedpostx.py <output dir>
|                                 Generate streamlines (~15 minutes with GPU)
|
+- s2b_freesurfer.py            # ./s2b_freesurfer.py <output dir>
|                                 Map regions (~3 hours with GPU and 36 cores)
|
+- s3_ediPreproc.py             # ./s3_ediPreproc.py <output dir>  
|                                 Generate connectome and analyze tractography (~3 hours with 36 cores)
|
+- s4_consensusEDI.py           # ./s4_consensusEDI.py <output dir>
|                                 Generate edge density image (~1 hour)
+- scheduleContainer/
|  +- genScripts.py             # ./scheduleContainer/genScripts.py <input dir> <output dir>
|                                 Generate Moab batch scripts using container
|                                 For example, to submit: msub scheduleContainer/s1_dtiPreproc.qsub
+- scheduleLocal/
|  +- genScripts.py             # ./scheduleLocal/genScripts.py <input dir> <output dir>
|                                 Generate Moab batch scripts using local install
|                                 For example, to submit: msub scheduleLocal/s1_dtiPreproc.local.qsub
+- subscripts/
   +- __init__.py
   +- maskseeds.py              # See s2b_freesurfer.py
   +- utilities.py              # General utility functions
```