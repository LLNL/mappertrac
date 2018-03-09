from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *

if len(argv) < 3:
    print "Usage: %s <freeSurfer-dir> <T1.nii.gz> [force]" % argv[0]
    exit(0)

output_dir = split(abspath(argv[2]))[0]
subject = split(output_dir)[1]

# Shall we force a re-computation
force = ((len(argv) > 3) and argv[3] == 'force')

# Make the output directories if necessary    
if not exists(join(output_dir,"mri")):
    mkdir(join(output_dir,"mri"))

if not exists(join(output_dir,"mri/orig")):
    mkdir(join(output_dir,"mri/orig"))

fs_dir = abspath(argv[1])
environ['FREESURFER_HOME'] = fs_dir
environ['SUBJECTS_DIR'] = split(output_dir)[0]

if force or not exists(join(output_dir,"mri/orig/001.mgz")):
    system(join(fs_dir,'bin/mri_convert') + " %s %s" % (abspath(argv[2]),join(output_dir,"mri/orig/001.mgz")))


system(join(fs_dir,'bin/recon-all') + " -s %s -all -no-isrunning" % subject)


    
    
            


