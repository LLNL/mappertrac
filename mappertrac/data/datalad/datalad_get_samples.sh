#!/bin/bash
#
# Usage: datalad_get_sample.sh {path for data}
#     Requires Datalad installation: 
#         For local install (with sudo ability for git-annex installer):
#             pip install datalad-installer && datalad-installer git-annex -m datalad/packages && pip install datalad
#     -or-  
#         Using datalad container (see mappertrac/data/container/datalad.def and build.sh) from parent directory for sample data:
#             singularity exec -B ./:/dataout,./datalad_get_sample.sh:/scripts/datalad_get_sample.sh ../container/datalad.sif bash /scripts/datalad_get_sample.sh /dataout/
# 

# go to data path provided in command
data_dir="${1}"
cd ${data_dir}

# clone git of Traveling Human Phantom (THP) dataset (https://doi.org/10.18112/openneuro.ds000206.v1.0.0)
datalad clone https://github.com/OpenNeuroDatasets/ds000206.git
cd ds000206
# get data from MGH
datalad get sub-THP0001/ses-THP0001MGH1
mkdir ./ds000206/.ignore/
mv ./ds000206/sub-THP0001/ses-THP0001MGH1/dwi/sub-THP0001_ses-THP0001MGH1_acq-GD31_run-02_dwi* ./ds000206/.ignore/
mv ./ds000206/sub-THP0001/ses-THP0001MGH1/dwi/sub-THP0001_ses-THP0001MGH1_acq-GD31_run-03_dwi* ./ds000206/.ignore/
mv ./ds000206/sub-THP0001/ses-THP0001MGH1/dwi/sub-THP0001_ses-THP0001MGH1_acq-GD31_run-04_dwi* ./ds000206/.ignore/
mv ./ds000206/sub-THP0001/ses-THP0001MGH1/dwi/sub-THP0001_ses-THP0001MGH1_acq-GD79_run-01_dwi* ./ds000206/.ignore/
mv ./ds000206/sub-THP0001/ses-THP0001MGH1/dwi/sub-THP0001_ses-THP0001MGH1_acq-GD79_run-02_dwi* ./ds000206/.ignore/
rm -rf ./ds000206/sub-THP0001/ses-THP0001C*
rm -rf ./ds000206/sub-THP0001/ses-THP0001DART1
rm -rf ./ds000206/sub-THP0001/ses-THP0001UW1
rm -rf ./ds000206/sub-THP0001/ses-THP0001UMN1
rm -rf ./ds000206/sub-THP0001/ses-THP0001UCI1
rm -rf ./ds000206/sub-THP0001/ses-THP0001JHU1
rm -rf ./ds000206/sub-THP0001/ses-THP0001IOWA*
