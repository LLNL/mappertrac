from sys import argv
from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ,rmdir
from shutil import *
from utilities import *

def smart_copy(src,dest,force=False):
    if force or not exists(dest):
        copyfile(src,dest)
 
if len(argv) < 3:
    print "Usage: %s <target-dir> <subject-id> [force]" % argv[0]
    exit(0)

# Check whether the right environment variables are set
FSLDIR = join(environ["FSLDIR"],"bin")
if not exists(FSLDIR):
    print "Cannot find FSL_DIR environment variable"
    exist(0)


# Shall we force a re-computation
force = ((len(argv) > 3) and argv[3] == 'force')



# root directory
root = join(abspath(argv[1]),argv[2])

if not exists(join(root,"bedpostx_b1000")):
    mkdir(join(root,"bedpostx_b1000"))

bedpostx = join(root,"bedpostx_b1000")


smart_copy(join(root,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"),force)
smart_copy(join(root,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"),force)
smart_copy(join(root,"bvals"),join(bedpostx,"bvals"),force)
smart_copy(join(root,"bvecs"),join(bedpostx,"bvecs"),force)

print force            
if force or not exists(join(root,"bedpostx_b1000.bedpostX","dyads3.nii.gz")):
    
    if exists(join(root,"bedpostx_b1000.bedpostX")):
        rmtree(join(root,"bedpostx_b1000.bedpostX"))
            
    
    if exists(join(FSLDIR,"bedpostx_gpu")):
        run("bedpostx_gpu",  " " + bedpostx)
    else:
        run("bedpostx",  " " + bedpostx)
  

