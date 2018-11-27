#!/bin/bash

# If using local libraries, first run this script in the current shell
# Example usage: source configLocal.sh /usr/workspace/wsb/tbidata/surface/fsl /usr/workspace/wsb/tbidata/surface/freesurfer /usr/tce/packages/cuda/cuda-8.0/lib64

export FSLDIR=$1
. $FSLDIR/etc/fslconf/fsl.sh

export FREESURFER_HOME=$2
source $FREESURFER_HOME/SetUpFreeSurfer.sh

export SUBJECTS_DIR=${FREESURFER_HOME}/subjects

# CUDA parameters are optional
CUDA_8_LIB_DIR=${3:-}
if [ ! -z $CUDA_8_LIB_DIR ]; then
    export CUDA_8_LIB_DIR=${CUDA_8_LIB_DIR}:
    module load cuda/8.0
    export COMPILE_GPU=1
fi
CUDA_5_LIB_DIR=${4:-}
if [ ! -z $CUDA_5_LIB_DIR ]; then
    export CUDA_5_LIB_DIR=${CUDA_5_LIB_DIR}:
fi

export LD_LIBRARY_PATH=$FREESURFER_HOME:${FSLDIR}/lib:${FSLDIR}/bin:${CUDA_8_LIB_DIR}${CUDA_5_LIB_DIR}$LD_LIBRARY_PATH

export OMP_NUM_THREADS=36

export PARSL_TRACKING=false

