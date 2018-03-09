from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *

def smart_copy(src,dest,force=False):
    if force or not exists(dest):
        copyfile(src,dest)
 
if len(argv) < 4:
    print "Usage: %s <source-dir> <target-dir> <subject-id> [force]" % argv[0]
    exit(0)

# Check whether the right environment variables are set
FSL_DIR = join(environ["FSL_DIR"],"bin")
if not exists(FSL_DIR):
    print "Cannot find FSL_DIR environment variable"
    exist(0)


# Shall we force a re-computation
force = ((len(argv) > 5) and argv[4] == 'force')



# source directory
sdir = join(abspath(argv[1]),argv[3])

# make sure the target dir exists
if not exists(abspath(argv[2])):
    mkdir(abspath(argv[2]))

if not exists(join(abspath(argv[2]),argv[3])):
    mkdir(join(abspath(argv[2]),argv[3]))

# patient directory
pdir = join(abspath(argv[2]),argv[3])

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


# If necessary copy the data into the targte directory 
smart_copy(join(sdir,"hardi.nii.gz"),data,force)
smart_copy(join(sdir,"bvecs"),bvecs)
smart_copy(join(sdir,"bvals"),bvals)
              


# Start the eddy correction
if force or not exists(eddy):
    
    if exists(eddy_log):
        remove(eddy_log)

    print FSL_DIR    
    print "Doing eddy correcttion" 
    system("time " + join(FSL_DIR,"eddy_correct") + " %s %s 0" % (data,eddy)) 

if force or not exists(bet):
   
    print "Doing brain extraction"
    #print "time " + join(FSL_DIR,"bet") + " %s %s -m -f 0.3" % (eddy,bet)
    system("time " + join(FSL_DIR,"bet") + " %s %s -m -f 0.3" % (eddy,bet))


if force or not exists(bvecs_rotated):
    print "Rotating"
    system("time " + join(FSL_DIR,"fdt_rotate_bvecs") + " %s %s %s" % (bvecs,bvecs_rotated,eddy_log))

if force or not exists(dti_params+"_MD.nii.gz"):

    system("time " + join(FSL_DIR,"dtifit") + " --verbose -k %s -o %s -m %s -r %s -b %s" % (eddy,dti_params,bet_mask,bvecs,bvals))
    system("time " + join(FSL_DIR,"fslmaths") + " %s -add %s -add %s -div 3 %s " % (dti_params+"_L1.nii.gz",dti_params+"_L2.nii.gz",dti_params+"_L3.nii.gz",dti_params+"_MD.nii.gz"))    
    system("time " + join(FSL_DIR,"fslmaths") + " %s -add %s  -div 2 %s " % (dti_params+"_L2.nii.gz",dti_params+"_L3.nii.gz",dti_params+"_RD.nii.gz"))

    copy(dti_params+"_L1.nii.gz",dti_params+"_AD.nii.gz")
    copy(dti_params+"_FA.nii.gz",fa)
    
