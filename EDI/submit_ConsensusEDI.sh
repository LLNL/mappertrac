#!/bin/bash
# Runs the EDI consensus connectome on a given subject list

#first input: file list
#second input: root dir
#third input: PBTK directory
#fourth input: list of edges
#fifth input: suffic to EDI map

subjList=/p/lscratchh/kaplan7/tbi_data/Code/TractographyCode_EDI_Connectome/edi_concensus_subj.txt
subjDir=/p/lscratchh/kaplan7/tbi_data/processed/
installDir=/p/lscratchh/kaplan7/tbi_data/Code/TractographyCode_EDI_Connectome

# Don't change the below
pbtkDir='tmp'
edgeList=$installDir/EDIedges_consensusDK_b1000.txt
ediSuffix='twoway'

$installDir/wrapper_consensusEDIedges.sh $subjList $subjDir $pbtkDir $edgeList $ediSuffix
