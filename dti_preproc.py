from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *
from utilities import *

def smart_copy(src,dest,force=False):
    if force or not exists(dest):
        copyfile(src,dest)
 
if len(argv) < 4:
    print "Usage: %s <source-dir> <target-dir> [force]" % argv[0]
    exit(0)

# Check whether the right environment variables are set
FSLDIR = join(environ["FSLDIR"],"bin")
if not exists(FSLDIR):
    print "Cannot find FSLDIR environment variable"
    exit(0)


# Shall we force a re-computation
force = ((len(argv) > 3) and argv[3] == 'force')

# source directory
sdir = abspath(argv[1])

# make sure the target dir exists
if not exists(abspath(argv[2])):
    mkdir(abspath(argv[2]))


# patient directory
pdir = abspath(argv[2])

# Create a list of files that this script is supposed to produce
# The target data file
data = join(pdir,"data.nii.gz")
eddy = join(pdir,"data_eddy.nii.gz")
eddy_log = join(pdir,"data_eddy.ecclog")
bet = join(pdir,"data_bet.nii.gz")
bet_mask = join(pdir,"data_bet_mask.nii.gz")
bvecs = join(pdir,"bvecs")
bvecs_rotated = join(pdir,"bvecs_rotated")
bvals = join(pdir,"bvals")
dti_params = join(pdir,"DTIparams")
fa = join(pdir,"FA.nii.gz")
T1 = join(pdir,"T1.nii.gz")

# If necessary copy the data into the targte directory 
smart_copy(join(sdir,"hardi.nii.gz"),data,force)
smart_copy(join(sdir,"bvecs"),bvecs)
smart_copy(join(sdir,"bvals"),bvals)
smart_copy(join(sdir,"anat.nii.gz"),T1)



# Start the eddy correction
if force or not exists(eddy):
    
    if exists(eddy_log):
        remove(eddy_log)

    print FSLDIR    
    print "Doing eddy correcttion" 
    run("eddy_correct"," %s %s 0" % (data,eddy)) 

if force or not exists(bet):
   
    print "Doing brain extraction"
    run("bet", " %s %s -m -f 0.3" % (eddy,bet))


if force or not exists(bvecs_rotated):
    print "Rotating"
    run("fdt_rotate_bvecs", " %s %s %s" % (bvecs,bvecs_rotated,eddy_log))

if force or not exists(dti_params+"_MD.nii.gz"):

    run("dtifit"," --verbose -k %s -o %s -m %s -r %s -b %s" % (eddy,dti_params,bet_mask,bvecs,bvals))
    run("fslmaths"," %s -add %s -add %s -div 3 %s " % (dti_params+"_L1.nii.gz",dti_params+"_L2.nii.gz",dti_params+"_L3.nii.gz",dti_params+"_MD.nii.gz"))    
    run("fslmaths", " %s -add %s  -div 2 %s " % (dti_params+"_L2.nii.gz",dti_params+"_L3.nii.gz",dti_params+"_RD.nii.gz"))

    copy(dti_params+"_L1.nii.gz",dti_params+"_AD.nii.gz")
    copy(dti_params+"_FA.nii.gz",fa)
    
