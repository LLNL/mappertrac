#!/usr/bin/env python3
import argparse
import time
from os.path import exists,join,splitext,abspath
from os import system,mkdir,remove,environ,chmod
import stat
from shutil import *
from glob import glob
from tempfile import *
from utilities import *

parser = argparse.ArgumentParser(description='Preprocess EDI data')
parser.add_argument('src_and_target', help='Region pair, in the form <source.nii.gz>:<target.nii.gz>')
parser.add_argument('output_dir', help='The directory where the output files should be stored')
parser.add_argument('--bedpost_dir', help='The directory containing bedpost output, relative to output directory', default='bedpostx_b1000.bedpostX')
parser.add_argument('--pbtk_dir', help='The directory to place region pair output, relative to output directory',
                    default=join("EDI","PBTKresults"))
parser.add_argument('--force', help='Force re-compute if output already exists', action='store_true')
args = parser.parse_args()

start_time = time.time()

# load_environ()
src, target = args.src_and_target.split(":", 1)
src_name = splitext(splitext(os.path.split(src)[1])[0])[0]
target_name = splitext(splitext(os.path.split(target)[1])[0])[0]

odir = abspath(args.output_dir)
bedpost_dir = join(odir, args.bedpost_dir)
if not exists(bedpost_dir):
    print("BedpostX directory does not exist at {}".format(bedpost_dir))
    exit(0)
save_dir = join(odir, args.pbtk_dir)
save_file = "{}to{}.nii.gz".format(src_name,target_name)
print("Running subproc: {} to {}".format(src_name, target_name))

if not exists(join(save_dir, save_file)) or args.force:

    tmp_dir = ""
    if 'LOGNAME' in os.environ and exists(join("/p/lscratchh", os.environ['LOGNAME'], "tmp")):
        tmp_dir = mkdtemp(dir=join("/p/lscratchh", os.environ['LOGNAME'], "tmp"))
    else:
        tmp_dir = mkdtemp()

    fsl = join(environ['FSLDIR'],"bin")

    print(src)
    print(target)
    copy(src,tmp_dir)
    copy(target,tmp_dir)
    copy(join(odir,"EDI","terminationmask.nii.gz"),tmp_dir)
    chmod(join(tmp_dir,"terminationmask.nii.gz"),stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)

    copy(join(odir,"EDI","allvoxelscortsubcort.nii.gz"),tmp_dir)

    copy(join(odir,"EDI","bs.nii.gz"),join(tmp_dir,"brainstemplate.nii.gz"))
    chmod(join(tmp_dir,"brainstemplate.nii.gz"),stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)

    copytree(bedpost_dir,join(tmp_dir,"bedpostx"))



    # Creating the masks
    run("fslmaths {} -sub {} {}".format(join(tmp_dir,"allvoxelscortsubcort.nii.gz"),
                                       join(tmp_dir,split(src)[1]),
                                       join(tmp_dir,"exclusion.nii.gz")))

    run("fslmaths {} -sub {} {}".format(join(tmp_dir,"exclusion.nii.gz"),
                                                    join(tmp_dir,split(target)[1]),
                                                    join(tmp_dir,"exclusion.nii.gz")))

    run("fslmaths {} -add {} {}".format(join(tmp_dir,"exclusion.nii.gz"),
                                                    join(tmp_dir,split(src)[1]),
                                                    join(tmp_dir,"brainstemplate.nii.gz")))

    run("fslmaths {} -add {} {}".format(join(tmp_dir,"terminationmask.nii.gz"),
                                                    join(tmp_dir,split(target)[1]),
                                                    join(tmp_dir,"terminationmask.nii.gz")))

    waypoint = open(join(tmp_dir,"waypoint.txt"),"w")
    waypoint.write(target + "\n")
    waypoint.close()


    arguments = (" -x {} ".format(join(tmp_dir,split(src)[1]))
        + " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000"
        + " --waypoints={}".format(join(tmp_dir,"waypoint.txt"))
        + " --avoid={}".format(join(tmp_dir,"exclusion.nii.gz"))
        + " --stop={}".format(join(tmp_dir,"terminationmask.nii.gz"))
        + " --forcedir --opd"
        + " -s {}".format(join(tmp_dir,"bedpostx","merged"))
        + " -m {}".format(join(tmp_dir,"bedpostx","nodif_brain_mask.nii.gz"))
        + " --dir={}".format(tmp_dir)
        + " --out={}".format(save_file)
        )

    run("probtrackx2" + arguments)
    copy(join(tmp_dir,save_file),join(save_dir))

    rmtree(tmp_dir)

print("{} to {}: {} seconds".format(src_name, target_name, time.time() - start_time))
