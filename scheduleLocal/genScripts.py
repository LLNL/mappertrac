#!/usr/bin/env python3
import argparse
parser = argparse.ArgumentParser(description='Generate connectome data')
parser.add_argument('input_dir', help='The directory with the input dataset '
                    'formatted according to the BIDS standard.')
parser.add_argument('output_dir', help='The directory where the output files '
                    'should be stored')
args = parser.parse_args()

def get_header(job_name, walltime, stdout):
    return """#!/bin/tcsh
#MSUB -N {}
#MSUB -l nodes=1
#MSUB -l walltime={}
#MSUB -q pbatch
#MSUB -A asccasc
#MSUB -o scheduleLocal/{}\n""".format(job_name, walltime, stdout)


with open("scheduleLocal/s1_dtiPreproc.local.qsub", "w") as f:
    f.write(get_header("tbi_s1", "10:00", "s1_dtiPreproc.local.stdout"))
    f.write("python3 s1_dtiPreproc.py {} {} --output_time --force\n".format(args.input_dir, args.output_dir))

with open("scheduleLocal/s2a_bedpostx.local.qsub", "w") as f:
    f.write(get_header("tbi_s2a", "30:00", "s2a_bedpostx.local.stdout"))
    f.write("module load cuda/8.0\n")
    f.write("python3 s2a_bedpostx.py {} --output_time --force\n".format(args.output_dir))

with open("scheduleLocal/s2b_freesurfer.local.qsub", "w") as f:
    f.write(get_header("tbi_s2b", "1:00:00", "s2b_freesurfer.local.stdout"))
    f.write("module load cuda/8.0\n")
    f.write("python3 s2b_freesurfer.py {} --output_time --force --use_gpu\n".format(args.output_dir))

with open("scheduleLocal/s3_ediPreproc.local.qsub", "w") as f:
    f.write(get_header("tbi_s3", "60:00", "s3_ediPreproc.local.stdout"))
    f.write("python3 s3_ediPreproc.py {} --output_time --force\n".format(args.output_dir))

with open("scheduleLocal/s4_consensusEDI.local.qsub", "w") as f:
    f.write(get_header("tbi_s4", "60:00", "s4_consensusEDI.local.stdout"))
    f.write("python3 s4_consensusEDI.py {} --output_time --force\n".format(args.output_dir))