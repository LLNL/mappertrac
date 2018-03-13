from sys import argv
from os.path import exists, join, split, splitext, abspath
from os import system, mkdir, remove, environ
from shutil import *
from glob import glob
from posix import remove


if len(argv) < 2:
    print "Usage: %s <output-dir> [force] " % argv[0]
    exit(0)


output_dir = abspath(argv[1])
EDI = join(output_dir, "EDI")
fsl = join(environ['FSL_DIR'], "bin")


# Shall we force a re-computation
force = ((len(argv) > 2) and argv[2] == 'force')


if not exists(join(EDI, "PBTKresultsbedpostx_b1000distcorr0")):
    mkdir(join(EDI, "PBTKresultsbedpostx_b1000distcorr0"))

if not exists(join(EDI, "PBTKresultsbedpostx_b1000_thalexcldistcorr0")):
    mkdir(join(EDI, "PBTKresultsbedpostx_b1000_thalexcldistcorr0"))

if force or not exists(join(output_dir, "bedpostx_b1000.bedpostX", "dyads2_dispersion.nii.gz")):
    bed_dir = join(output_dir, "bedpostx_b1000.bedpostX")
    system(join(fsl, "make_dyadic_vectors") + " %s %s %s %s" % (join(bed_dir, "merged_th1samples"),
                                                                join(bed_dir, "merged_ph1samples"),
                                                                join(bed_dir, "nodif_brain_mask"),
                                                                join(bed_dir, "dyads1")))

    system(join(fsl, "make_dyadic_vectors") + " %s %s %s %s" % (join(bed_dir, "merged_th2samples"),
                                                                join(bed_dir, "merged_ph2samples"),
                                                                join(bed_dir, "nodif_brain_mask"),
                                                                join(bed_dir, "dyads2")))

    rmtree(join(EDI, "bedpostx_b1000.bedpostX"))


if not exists(join(EDI, "bedpostx_b1000.bedpostX")):
    copytree(join(output_dir), EDI)



