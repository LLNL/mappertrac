# TRACK TBI

### Running Scripts
Requires a local install of FSL (https://fsl.fmrib.ox.ac.uk/fsl/fslwiki) and Freesurfer (https://surfer.nmr.mgh.harvard.edu/fswiki)

**OR**

Support for Singlarity containers, version 2.5+ (https://www.sylabs.io/guides/2.5/user-guide)

```
IMPORTANT: Either way, always call scripts from the repo's base directory
```

### Building the Singularity Image ###

Though Singularity can run with limited privileges, building an image requires root privileges. From the base directory, simply run "./container/build.sh". This process requires internet connection and 10 GB free disk space. It may take several hours, depending on connection speed.

### File Overview

```
TracktographyScripts/
+- configLocal.sh               # source configLocal.sh <FSL dir> <Freesurfer dir> <CUDA lib dir (optional)> 
|                                 IMPORTANT: Run this before using local install
+- container/
|  +- internal/
|  |  +- fslinstaller.py        # Custom FSL install script (do not change)
|  |  +- run.py                 # Container runscript (do not change)
|  |
|  +- image.simg                # Not included
|  |                              Singularity container with FSL & Freesurfer, roughly 7 GB size
|  |
|  +- run.sh                    # ./container/run.sh <command>
|  |                              Run command in container
|  |
|  +- runGPU.sh                 # ./container/runGPU.sh <command>
|  |                              Run GPU-enabled command in container, requires Nvidia Tesla GPU and CUDA 8.0
|  |
|  +- shell.sh                  # ./container/shell.sh
|                                 Open shell in container
|  
+- license.txt                  # Not included, required to use container
|                                 Download at https://surfer.nmr.mgh.harvard.edu/fswiki/License
+- lists/
|  +- listEdgesEDI.txt          # See s4_consensusEDI.py
|  +- subcorticalIndex.txt      # See s2b_freesurfer.py
+- README
+- s1_dtiPreproc.py             # ./s1_dtiPreproc.py <input dir> <output dir>
|                                 Setup and image correction (~5 minutes with 36 cores)
|
+- s2a_bedpostx.py              # ./s2a_bedpostx.py <output dir>
|                                 Generate streamlines (~30 minutes with GPU)
|
+- s2b_freesurfer.py            # ./s2b_freesurfer.py <output dir>
|                                 Map regions (~10 hours)
|
+- s3_ediPreproc.py             # ./s3_ediPreproc.py <output dir>  
|                                 Generate connectome and analyze tractography (~3 hours)
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