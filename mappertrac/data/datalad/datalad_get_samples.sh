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
cd ${dataout}

# clone git of Traveling Human Phantom (THP) dataset (https://doi.org/10.18112/openneuro.ds000206.v1.0.0)
datalad clone https://github.com/OpenNeuroDatasets/ds000206.git
cd ds000206
# get data from MGH
datalad get sub-THP0001/ses-THP0001MGH1
