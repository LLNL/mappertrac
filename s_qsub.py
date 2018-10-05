#!/usr/bin/env python3
import argparse
import sys
from subscripts.utilities import *
from os.path import exists,join,split,splitext,abspath,basename,isdir
from os import system,mkdir,remove,environ,makedirs
from math import floor,ceil

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

parser = argparse.ArgumentParser(description='Run parallel script using PBSBatchMaster')
parser.add_argument('subject_list',help='Text file with list of subject directories.')
parser.add_argument('output_dir',help='The super-directory that will contain output directories for each subject')
parser.add_argument('script',help='Script to run in parallel')
parser.add_argument('--force',help='Force re-compute if output already exists',action='store_true')
parser.add_argument('--output_time',help='Print completion time',action='store_true')
parser.add_argument('--use_input',help='Add an input argument before the subject dir argument',action='store_true')
parser.add_argument('--max_nodes',help='Max nodes per Moab batch script',default=2)
args = parser.parse_args()

# cores_per_task = 36
max_time = 10
qsub = join('qsub','arguments')

force = "--force" if args.force else ""
output_time = "--output_time" if args.output_time else ""

odir = abspath(args.output_dir)
if not isdir(odir):
    makedirs(odir)

if not isdir("qsub"):
    mkdir("qsub")

lines = []
with open(args.subject_list) as f:
    for line in f.readlines():
        lines.append(line.strip())
input_dirs = list(chunks(lines, args.max_nodes))

i=0
for chunk in input_dirs:
    with open('{}{}.list'.format(qsub,i),'w') as f:
        for input_dir in chunk:
            sdir = join(odir, basename(input_dir))
            if not isdir(sdir):
                mkdir(sdir)
            if args.use_input:
                f.write(input_dir + ' ' + sdir + '\n')
            else:
                f.write(sdir + '\n')
    i+=1

i=0
for chunk in input_dirs:
    pbs_cmd = ("python batch_scripts/PBSBatchMaster.py" 
               + " {}{}.list {} {}".format(qsub,i,len(chunk),max_time)
               + " python3 {}".format(args.script)
               + " --input {} {}".format(force, output_time)
               )
    run(pbs_cmd)
    i+=1

