#!/bin/bash

# If running outside Singularity, run this script before any others
# Example usage: ./configLocal.sh /usr/workspace/wsb/tbidata/surface/fsl /usr/workspace/wsb/tbidata/surface/freesurfer /usr/tce/packages/cuda/cuda-8.0/lib64

export FSLDIR=$1
. $FSLDIR/etc/fslconf/fsl.sh

module load cuda/8.0

export COMPILE_GPU=1

export FREESURFER_HOME=$2
source $FREESURFER_HOME/SetUpFreeSurfer.sh

export SUBJECTS_DIR=${FREESURFER_HOME}/subjects

export CUDA_LIB_DIR=$3

export LD_LIBRARY_PATH=$FREESURFER_HOME:${FSLDIR}/bin:$CUDA_LIB_DIR:$LD_LIBRARY_PATH

