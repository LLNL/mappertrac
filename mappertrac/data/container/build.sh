#!/usr/bin/env bash
sudo rm -rf ./*.sif
sudo singularity build fsl_tractography.sif fsl_tractography.def
sudo singularity build freesurfer-v6.0.0.sif fsl_freesurfer.def
sudo singularity build mrtrix3-v3.0.3.sif mrtrix3.def
