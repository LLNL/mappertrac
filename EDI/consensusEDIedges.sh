#!/bin/bash
#first input root dir
#second input PBTK directory
#third input list of edges
#fourth input sufffix for EDI nifti

echo $1
cd $1/EDI

if [ ! -d EDImaps ]
then
	mkdir EDImaps
fi

if [ ! -e EDImaps/FAtractsums$4.nii.gz ]
then
	cd $2

	mkdir twoway_consensus_edges
	#mkdir twoway_consensus_edges_T1

	n=1
	for line in ` cat $3 `
	do
		echo $n
	
		a=` echo $line | cut -f 1 -d , `
		b=` echo $line | cut -f 2 -d , `
	
		if [ ! -e twoway_consensus_edges/$a"to"$b".nii.gz" ]
		then
	
			amax=` fslstats $a"to"$b".nii.gz" -R | cut -f 2 -d " " `
			bmax=` fslstats $b"to"$a".nii.gz" -R | cut -f 2 -d " " `
			amax=${amax%.*}
			bmax=${bmax%.*}
	
			if [ $amax -gt 0 -a $bmax -gt 0 ]
			then
				echo $amax 
				echo $bmax 
				rm tmp*
				fslmaths $a"to"$b".nii.gz" -thrP 5 -bin ${4}"tmp1.nii.gz"
				fslmaths $b"to"$a".nii.gz" -thrP 5 -bin ${4}"tmp2.nii.gz"
				fslmaths ${4}"tmp1.nii.gz" -add ${4}"tmp2.nii.gz" -thr 1 -bin twoway_consensus_edges/$a"to"$b".nii.gz"
				rm ${4}"tmp1.nii.gz"
				rm ${4}"tmp2.nii.gz"
				#flirt -in twoway_consensus_edges/$a"to"$b".nii.gz" -ref ../../T1_2.nii.gz -applyxfm -init ../FA2T1_2.mat -out twoway_consensus_edges_T1/$a"to"$b".nii.gz"
				#fslmaths twoway_consensus_edges_T1/$a"to"$b".nii.gz" -bin twoway_consensus_edges_T1/$a"to"$b".nii.gz"
			
				else
					echo $line >> zerosl.txt
					echo $amax >> zerosl.txt
					echo $bmax >> zerosl.txt
				fi
			else
				echo "edge made"
			fi
	
			echo $line
			if [ -e twoway_consensus_edges/$a"to"$b".nii.gz" ]
			then
				echo "hi hi"
			if [ $n -eq 1 ]
			then
				echo "copying first edge"
				cp twoway_consensus_edges/$a"to"$b".nii.gz" ../EDImaps/FAtractsums$4.nii.gz
			else
				echo "copy next edge"
				fslmaths twoway_consensus_edges/$a"to"$b".nii.gz" -add ../EDImaps/FAtractsums$4.nii.gz ../EDImaps/FAtractsums$4.nii.gz
			fi
			let n=n+1
		fi
		
	done
fi
