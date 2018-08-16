#!/bin/bash

#first input: file list
#second input: root dir
#third input: PBTK directory
#fourth input: list of edges
#fifth input: suffic to EDI map
# ../../HCP35/code/wrapper_consensusEDIedges $PWD/8002.101.txt $PWD PBTK_results_specificexl_b1000EDI_withdc /data/ncl-mb1/HCP35/EDIedges_consensus_onewayDK_b1000.txt oneway

for d in ` cat $1 `
do
	echo $d
	if [ ! -e $d/EDI/T1tractsums$4.nii.gz ]
	then
	/p/lscratchh/kaplan7/tbi_data/Code/TractographyCode_EDI_Connectome/consensusEDIedges.sh $2/$d $3 $4 $5
	fi
done
