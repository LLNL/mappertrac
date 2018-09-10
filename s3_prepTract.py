#!/usr/bin/env python3
import argparse
import time
import sys    
import os
from os.path import exists, join, split, splitext, abspath
from os import system, mkdir, remove, environ
from shutil import *
from glob import glob
from posix import remove
from utilities import *

parser = argparse.ArgumentParser(description='Prepare tractography')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
parser.add_argument('--output_time', help='Print completion time', action='store_true')
args = parser.parse_args()

start_time = time.time()
print("Running {}".format(os.path.basename(sys.argv[0])))
odir = abspath(args.output_dir)
EDI = join(odir, "EDI")

if not exists(join(EDI, "PBTKresultsbedpostx_b1000distcorr0")):
    mkdir(join(EDI, "PBTKresultsbedpostx_b1000distcorr0"))

if not exists(join(EDI, "PBTKresultsbedpostx_b1000_thalexcldistcorr0")):
    mkdir(join(EDI, "PBTKresultsbedpostx_b1000_thalexcldistcorr0"))

if args.force or not exists(join(odir, "bedpostx_b1000.bedpostX", "dyads2_dispersion.nii.gz")):
    bed_dir = join(odir, "bedpostx_b1000.bedpostX")
    run("make_dyadic_vectors {} {} {} {}".format(join(bed_dir, "merged_th1samples"),
                                                                join(bed_dir, "merged_ph1samples"),
                                                                join(bed_dir, "nodif_brain_mask"),
                                                                join(bed_dir, "dyads1")))

    run("make_dyadic_vectors {} {} {} {}".format(join(bed_dir, "merged_th2samples"),
                                                                join(bed_dir, "merged_ph2samples"),
                                                                join(bed_dir, "nodif_brain_mask"),
                                                                join(bed_dir, "dyads2")))

    rmtree(join(EDI, "bedpostx_b1000.bedpostX"))


if not exists(join(EDI, "bedpostx_b1000.bedpostX")):
    copytree(join(odir,"bedpostx_b1000.bedpostX"), join(EDI, "bedpostx_b1000.bedpostX"))

if args.output_time:
    print("{} took {}".format(os.path.basename(sys.argv[0]), getTimeString(time.time() - start_time)))


