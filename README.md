# TRACK TBI

### Running Scripts
Requires a local install of: 
* Python 3.5+
* FSL (https://fsl.fmrib.ox.ac.uk/fsl/fslwiki)
* Freesurfer (https://surfer.nmr.mgh.harvard.edu/fswiki)
* Parsl (http://parsl-project.org/)
* Bedpostx GPU (https://users.fmrib.ox.ac.uk/~moisesf/Bedpostx_GPU/index.html)
* CUDA 6.5-9.2, for Bedpostx GPU

With optional libraries:
* CUDA 5.0, for Freesurfer GPU

**OR**

Support for Singlarity containers, version 2.6.0 (https://www.sylabs.io/guides/2.6/user-guide)

### Building the Singularity Container ###

1. Make sure you have root access (you can copy the image to a non-root system afterwards).
2. Place a Freesurfer license in the repo directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).
3. Install Singularity 2.6.0 (https://github.com/sylabs/singularity/releases/tag/2.6.0)
4. From the repo directory, run "./container/build.sh"

### File Overview

```
TracktographyScripts/
+- clean.sh                     # Delete the checkpoint, logging, and helper files generated after running scripts
|
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
|  +- subcorticalIndex.txt      # List of regions post-processed in Freesurfer step
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