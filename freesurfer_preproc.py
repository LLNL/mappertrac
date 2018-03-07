from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *

if len(argv) < 4:
    print "Usage: %s <FreeSurfer-dir> <T1.nii.gz> <output-dir>" % argv[0]
    exit(0)

output_dir = abspath(argv[3])

# Make the output directories if necessary    
if not exists(join(output_dir,"mri")):
    mkdir(join(output_dir,"mri"))

if not exists(join(output_dir,"mri/orig")):
    mkdir(join(output_dir,"mri/orig"))

fs_dir = abspath(argv[1])
environ['FREESURFER_HOME'] = fs_dir
environ['SUBJECTS_DIR'] = split(argv[3])[0]

if not exists(join(output_dir,"mri/orig/001.mgz")):
    system(join(fs_dir,'bin/mri_convert') + " %s %s" % (abspath(argv[2]),join(output_dir,"mri/orig/001.mgz")))


system(join(fs_dir,'bin/recon-all') + " -s %s -all -no-isrunning" % split(argv[3])[1])
    
    
            


