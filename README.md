# TRACK TBI
Parallel EDI tractography workflow
  
  
### Setup

Requirements:
* Python 3.5+
* SLURM job scheduling on a multi-node system

1. Install NumPy and Parsl (http://parsl-project.org/)

`pip3 install parsl numpy`

2a. Install local libraries

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
  
2b. Load a Singularity container

Requirements:
* Singularity 3.0+ (https://www.sylabs.io/guides/3.0/user-guide/)
* Nvidia Tesla GPU hardware

Building the container:
i. Obtain root access (you can copy the image to a non-root system afterwards).
ii. Place a Freesurfer `license.txt` in the repo directory (https://surfer.nmr.mgh.harvard.edu/fswiki/License).
iii. `./container/build.sh`
  
  
### Launch

`./s_run_all.py <config_file>`

**OR**

`./s_run_all.py <arg1> <arg2> etc...`
  
  
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
|  +- render_targets.txt        # List of NiFTI images to visualize with s5_render.py
|
+- README.md
|
+- s_run_all.py                 # Main script
|
+- subscripts/
   +- __init__.py
   +- maskseeds.py              # Helper functions for s2b_freesurfer.py
   +- run_vtk.py                # Helper script for s5_render.py
   +- s_debug.py                # Minimal debug step
   +- s1_dti_preproc.py
   +- s2a_bedpostx.py
   +- s2b_freesurfer.py
   +- s3_probtrackx.py
   +- s4_edi.py
   +- s5_render.py
   +- utilities.py              # General utility functions
```
