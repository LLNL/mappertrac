#!/usr/bin/env bash
sudo rm -rf ./*.sif
sudo singularity build fsl-v6.0.5.1.sif fsl_v6.0.5.1.def
sudo singularity build freesurfer-v7.3.2.sif freesurfer_v7.3.2.def
sudo singularity build mrtrix3-v3.0.3.sif mrtrix3.def
