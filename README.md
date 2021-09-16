mappertrac
===========

mappertrac is a probabilistic tractography workflow using structural DW-MRI and designed for high performance computing.

Inputs: structural DW-MRI `.nii.gz`, T1-weighted anatomical MRI `.nii.gz`, `.bval`, `.bvec`

Outputs: connectome matrix `.mat` and edge density `.nii.gz`

---

## Quick Setup

#### Requirements

* Python 3.7+
* [Singularity](https://sylabs.io/guides/3.5/user-guide/index.html)
* CUDA-capable GPU

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

---

## Instructions

#### Example Testing
Check that mappertrac works on your system by running the example input data.
```
mappertrac --s1_freesurfer --test
```

#### Multiple subjects
You can specify multiple subjects with specific paths or Unix-style globbing
```
mappertrac --s1_freesurfer <SUBJECT1_DIR> <SUBJECT2_DIR> <SUBJECT3_DIR>
mappertrac --s1_freesurfer <ALL_SUBJECTS_DIR>/*/
```

#### Slurm scheduling
Multiple subjects can be run on distributed systems using Slurm or Flux.
```
mappertrac --s1_freesurfer --slurm -n 1 -b mybank -p mypartition <SUBJECT_INPUT_DIRECTORY>
```

#### Additional options
```
mappertrac --help
```

---

License
-------
mappertrac is distributed under the terms of the BSD-3 License.

LLNL-CODE-811655
