#!/usr/bin/env bash
#
# Usage: from directory for storing singularity image builds (e.g. /opt/singularity_images/)
#   ./build.sh {yes/no for datalad}

# Datalad container install (yes or no)
DATALAD=$1

sudo rm -rf ./*.sif
sudo singularity build fsl-v6.0.6.4.sif fsl_v6.0.6.4.def
sudo singularity build freesurfer-v7.3.2.sif freesurfer_v7.3.2.def
sudo singularity build mrtrix3-v3.0.3.sif mrtrix3.def
# If you want to download test data from open-neuro, run with yes
if [ "${DATALAD}" == "yes" ]; then
  echo "Building Datalad container"
  echo "Use mappertrac/data/datalad/datalad_get_sample.sh to download a sample Siemens session from OpenNeuro"
  sudo singularity build datalad.sif datalad.def
fi
