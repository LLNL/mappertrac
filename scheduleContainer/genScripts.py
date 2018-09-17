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
#MSUB -o scheduleContainer/{}\n""".format(job_name, walltime, stdout)


with open("scheduleContainer/s1_dtiPreproc.qsub", "w") as f:
    f.write(get_header("tbi_s1", "45:00", "s1_dtiPreproc.stdout"))
    f.write("./container/run.sh s1_dtiPreproc.py {} {} --output_time --force\n".format(args.input_dir, args.output_dir))

with open("scheduleContainer/s2a_bedpostx.qsub", "w") as f:
    f.write(get_header("tbi_s2a", "20:00", "s2a_bedpostx.stdout"))
    f.write("module load cuda/8.0\n")
    f.write("./container/runGPU.sh s2a_bedpostx.py {} --output_time --force\n".format(args.output_dir))

with open("scheduleContainer/s2b_freesurfer.qsub", "w") as f:
    f.write(get_header("tbi_s2b", "12:00:00", "s2b_freesurfer.stdout"))
    f.write("./container/run.sh s2b_freesurfer.py {} --output_time --force\n".format(args.output_dir))

with open("scheduleContainer/s3_ediPreproc.qsub", "w") as f:
    f.write(get_header("tbi_s3", "60:00", "s3_ediPreproc.stdout"))
    f.write("./container/run.sh s3_ediPreproc.py {} --output_time --force\n".format(args.output_dir))

with open("scheduleContainer/s4_consensusEDI.qsub", "w") as f:
    f.write(get_header("tbi_s4", "60:00", "s4_consensusEDI.stdout"))
    f.write("./container/run.sh s4_consensusEDI.py {} --output_time --force\n".format(args.output_dir))