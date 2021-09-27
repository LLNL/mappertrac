mappertrac
===========
![example workflow](https://github.com/LLNL/MaPPeRTrac/actions/workflows/github-actions.yml/badge.svg
) [![PyPI version](https://badge.fury.io/py/mappertrac.svg)](https://badge.fury.io/py/mappertrac
) [![DOI](https://zenodo.org/badge/376166124.svg)](https://zenodo.org/badge/latestdoi/376166124)

mappertrac is a probabilistic tractography workflow using structural DW-MRI and designed for high performance computing.

Inputs: structural DW-MRI `.nii.gz`, T1-weighted anatomical MRI `.nii.gz`, `.bval`, `.bvec`

Outputs: connectome matrix `.mat` and edge density `.nii.gz`

---

## Quick Setup

#### Requirements

* Python 3.7+
* [Singularity](https://sylabs.io/guides/3.5/user-guide/index.html)
* CUDA 8.0-compatible GPU ([Kepler thru Turing](https://docs.nvidia.com/deploy/cuda-compatibility/))

#### Installation
```
pip3 install mappertrac
wget -O image.sif https://www.dropbox.com/s/2v74hra04bo22w7/image.sif?dl=1
```

#### Usage
```
mappertrac --s1_freesurfer <SUBJECT_INPUT_DIRECTORY>
mappertrac --s2_bedpostx <SUBJECT_INPUT_DIRECTORY>
mappertrac --s3_probtrackx <SUBJECT_INPUT_DIRECTORY>
```

Note: the input directory must adhere to [BIDS](https://bids.neuroimaging.io/). See this [example](https://github.com/LLNL/MaPPeRTrac/tree/master/mappertrac/data/example_inputs/sub-011591).

---

## Instructions

#### Conda Installation
If you're having trouble installing mappertrac, please use a clean environment using virtualenv or conda.
```
conda create -n myenv
conda activate myenv
pip install mappertrac
```

#### Example Testing
Check that your installation works by running the example input data.
```
mappertrac --s1_freesurfer --test
```

#### Multiple subjects
You can specify multiple subjects with specific paths or Unix-style globbing
```
mappertrac --s1_freesurfer <SUBJECT1_DIR> <SUBJECT2_DIR> <SUBJECT3_DIR>
mappertrac --s1_freesurfer <ALL_SUBJECTS_DIR>/*/
```

#### Job scheduling
Multiple subjects can be run on distributed systems using Slurm, Cobalt, or Grid Engine.
```
mappertrac --s1_freesurfer --slurm -b mybank -p mypartition <SUBJECT_INPUT_DIRECTORY>
```

#### Additional options
```
mappertrac --help
```
```
usage: mappertrac [-h] [--test] (--freesurfer | --bedpostx | --probtrackx) [--outputs OUTPUTS] [--container CONTAINER]
                  [--pbtx_sample_count PBTX_SAMPLE_COUNT] [--slurm | --cobalt | --grid_engine] [--nnodes NNODES] [--bank BANK]
                  [--partition PARTITION] [--walltime WALLTIME]
                  inputs [inputs ...]

positional arguments:
  inputs                Paths to BIDS subject folder(s).

optional arguments:
  -h, --help            show this help message and exit
  --test                Test using the example subject.
  --freesurfer, --s1_freesurfer, -s1
                        Run step 1: freesurfer.
  --bedpostx, --s2_bedpostx, -s2
                        Run step 2: bedpostx.
  --probtrackx, --s3_probtrackx, -s3
                        Run step 3: probtrackx.
  --outputs OUTPUTS, -o OUTPUTS
                        Path to output directories.
  --container CONTAINER
                        Path to Singularity container image.
  --pbtx_sample_count PBTX_SAMPLE_COUNT
                        Number of probtrackx samples per voxel.
  --slurm               Use the Slurm scheduler.
  --cobalt              Use the Cobalt scheduler.
  --grid_engine         Use the Grid Engine scheduler.
  --nnodes NNODES, -n NNODES
                        Scheduler: number of nodes.
  --bank BANK, -b BANK  Scheduler: bank to charge for jobs.
  --partition PARTITION, -p PARTITION
                        Scheduler: partition to assign jobs.
  --walltime WALLTIME, -t WALLTIME
                        Scheduler: walltime in format HH:MM:SS.
```

---

License
-------
mappertrac is distributed under the terms of the BSD-3 License.

LLNL-CODE-811655
