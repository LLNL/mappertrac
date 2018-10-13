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

~~Support for Singlarity containers, version 2.5+ (https://www.sylabs.io/guides/2.5/user-guide)~~ TODO: Get Singularity working with Parsl script

```
IMPORTANT: Either way, always call scripts from the repo's base directory
```

### Building the Singularity Container ###

Though Singularity can run with limited privileges, building a container requires root access. 

From the base directory, simply run "./container/build.sh". This process requires internet connection and 10 GB free disk space. It may take several hours, depending on connection speed.

If you do not have root access (e.g. using a cluster), then clone this repo on a system where you do. After building, copy "container/image.simg" into the repo on the desired system.

Make sure to place a Freesurfer license in the base directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).

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
|  +- listEdgesEDI.txt          # List of default edges to compute with Probtrackx (930 edges)
|  +- listEdgesEDIAll.txt       # List of all possible edges (6643 edges)
|  +- listEdgesEDITest.txt      # List of test edges (10 edges)
|  +- subcorticalIndex.txt      # List of regions post-processed in Freesurfer step
+- README.md
|
+- s_all_parsl.py             # ./s_all_parsl.py <subject_list> <output_dir> <step_choice (s1, s2a, s2b, or s3)>
|                                 Main script, runs specified subjects through the given step. Edit this file to configure Slurm and Parsl settings.
|
+- subscripts/
   +- __init__.py
   +- maskseeds.py              # See s2b_freesurfer.py
   +- utilities.py              # General utility functions
```