#!/usr/bin/env python3
import argparse
import re
import sys    
import os
import parsl
from os import system,mkdir,remove,environ
from subscripts.utilities import *
from tempfile import *
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.threads import ThreadPoolExecutor

parser = argparse.ArgumentParser(description='Preprocess Freesurfer data')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--pbtk_dir', help='The directory with inputs from bedpostX and PBTK processing, relative to output directory',
                    default=join("EDI","PBTKresults"))
parser.add_argument('--edge_list', help='Text list of edges for consensus EDI', default="lists/listEdgesEDI.txt")
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

if not exists(args.edge_list):
    raise Exception(args.edge_list + " not found")

start_time = printStart()

config = Config(
    executors=[
        ThreadPoolExecutor(
            max_threads=multiprocessing.cpu_count(),
            label='local_threads'
        )
    ],
    retries=3,
    checkpoint_mode = 'dfk_exit'
)
parsl.load(config)

odir = abspath(args.output_dir)
pdir = join(odir, args.pbtk_dir)
if not exists(pdir):
    print("PBTK directory does not exist at {}".format(pdir))
    exit(0)

edge_prefix = "twoway"
total = join(odir,"EDI","EDImaps","FAtractsums{}.nii.gz".format(edge_prefix))
if not args.force and exists(total):
    print("Consensus EDI output already exists. Use --force argument to re-compute.")
    exit(0)

# Make the output directories if necessary
if not exists(join(odir,"EDI")):
    mkdir(join(odir,"EDI"))
if not exists(join(odir,"EDI","EDImaps")):
    mkdir(join(odir,"EDI","EDImaps"))
if not exists(pdir):
    mkdir(pdir)
if not exists(join(pdir,"twoway_consensus_edges")):
    mkdir(join(pdir,"twoway_consensus_edges"))
tmp_dir = mkdtemp()

@python_app
def subproc(pdir,a_to_b,b_to_a):
    from subscripts.utilities import run,isFloat
    from os import remove
    from os.path import exists,join,splitext,abspath,split
    input_a_b = join(pdir,a_to_b)
    input_b_a = join(pdir,b_to_a)
    output = join(pdir,"twoway_consensus_edges",a_to_b)
    amax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(input_a_b), print_output=False, working_dir=pdir).strip()
    if not isFloat(amax):
        print("fslstats on " + a_to_b + " returns invalid value")
        continue
    amax = int(float(amax))

    bmax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(input_b_a), print_output=False, working_dir=pdir).strip()
    if not isFloat(bmax):
        print("fslstats on " + b_to_a + " returns invalid value")
        continue
    bmax = int(float(bmax))
    if amax > 0 and bmax > 0:
        tmp1 = join(pdir, "{}to{}_tmp1.nii.gz".format(a, b))
        tmp2 = join(pdir, "{}to{}_tmp2.nii.gz".format(b, a))
        run("fslmaths {} -thrP 5 -bin {}".format(input_a_b, tmp1), working_dir=pdir)
        run("fslmaths {} -thrP 5 -bin {}".format(input_b_a, tmp2), working_dir=pdir)
        run("fslmaths {} -add {} -thr 1 -bin {}".format(tmp1, tmp2, output), working_dir=pdir)
        remove(tmp1)
        remove(tmp2)
    else:
        with open(join(pdir, "zerosl.txt", "a")) as log:
            log.write(a_to_b)
            log.write(amax)
            log.write(bmax)

print("Starting edge consensus\n")
visited = []
jobs = []
with open(args.edge_list) as f:
    n=1
    for edge in f.readlines():
        edge = edge.replace("_s2fa", "")
        a, b = edge.strip().split(',', 1)
        a_to_b = "{}to{}.nii.gz".format(a, b)
        b_to_a = "{}to{}.nii.gz".format(b, a)
        if not a_to_b in visited and not b_to_a in visited:
            if not exists(join(pdir,a_to_b)):
                print("Edge from " + a + " to " + b + " not found")
                continue
            if not exists(join(pdir,b_to_a)):
                print("Edge from " + b + " to " + a + " not found")
                continue
            jobs.append(subproc(pdir,a_to_b,b_to_a))
            visited.append(a_to_b)
for job in jobs:
    try:
        job[1].result()
    except Exception as e:
        print("Failed job {}".format(job[0]))

copy(join(pdir,"twoway_consensus_edges",visited.pop_front()), total)
for a_to_b in visited:
    output = join(pdir,"twoway_consensus_edges",a_to_b)
    run("fslmaths {0} -add {1} {1}".format(output, total))

if args.output_time:
    printFinish(start_time)

