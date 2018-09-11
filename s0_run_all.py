#!/usr/bin/env python3
import os
import argparse
from utilities import *
from shutil import *
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ

def run_default(script_name, args, dirs):
	run("python3 -u {} {} {}".format(
		script_name, dirs, 
		"--force" if args.force else ""),
		output_time=True, name_override=script_name)

start_time = printStart()

parser = argparse.ArgumentParser(description="Generate connectome data")
parser.add_argument('input_dir', help='The directory with the input dataset '
                    'formatted according to the BIDS standard.')
parser.add_argument('output_dir', help='The directory where the output files '
                    'should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

if not exists(abspath("license.txt")) and not exists(join(environ['FREESURFER_HOME'], "license.txt")):
    print("Error: Running this software requires a Freesurfer license. Make sure to place license.txt here or in $FREESURFER_HOME.")
    print("You can obtain a license at https://surfer.nmr.mgh.harvard.edu/fswiki/License")

run_default('s1_dti_preproc.py', args, args.input_dir + ' ' + args.output_dir)
run_default('s2a_bedpostx.py', args, args.output_dir)
run_default('s2b_freesurfer_preproc.py', args, args.output_dir)
run_default('s3_prepTract.py', args, args.output_dir)
run_default('s4_edi_preproc.py', args, args.output_dir)
run_default('s5_consensusEDI.py', args, args.output_dir)

if args.output_time:
    printFinish(start_time)
