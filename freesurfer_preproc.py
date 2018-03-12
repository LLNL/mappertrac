from sys import argv
from os.path import exists,join,split,splitext,abspath
from os import system,mkdir,remove,environ
from shutil import *
from glob import glob


cortical_dir = "label_cortical"
vol_dir = "volumes_cortical"
sub_vol_dir = "volumes_subcortical"
threshold = "0.2"

if len(argv) < 3:
    print "Usage: %s <freeSurfer-dir> <T1.nii.gz> [force]" % argv[0]
    exit(0)

T1 = abspath(argv[2])
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
fsl = join(split(fs_dir)[0],"fsl")
environ['FREESURFER_HOME'] = fs_dir
environ['SUBJECTS_DIR'] = split(output_dir)[0]

if force or not exists(join(output_dir,"mri/orig/001.mgz")):
    system(join(fs_dir,'bin/mri_convert') + " %s %s" % (abspath(argv[2]),join(output_dir,"mri/orig/001.mgz")))


if force or not exists(join(output_dir,"mri","aparc+aseg.mgz")):
    system(join(fs_dir,'bin/recon-all') + " -s %s -all -no-isrunning" % subject)


if force or not exists(join(output_dir,abspath(argv[2]))):
    system(join(fs_dir,'bin/mri_convert') + " %s %s " % (join(output_dir,"mri","brain.mgz"),abspath(argv[2])))

    
if force or not exists(join(output_dir,"FA2T1.mat")):
    system(join(fsl,'bin/flirt') + " -in %s -ref %s -omat %s" % (join(output_dir,"FA.nii.gz"),abspath(argv[2]),join(output_dir,"FA2T1.mat")))
    
if force or not exists(join(output_dir,"T12FA.mat")):
    system(join(fsl,'bin/convert_xfm') + " -omat %s -inverse %s" % (join(output_dir,"T12FA.mat"),join(output_dir,"FA2T1.mat")))

if not exists(join(output_dir,"label_cortical")):
    mkdir(join(output_dir,"label_cortical"))    
    
# extract cortical labels (extralabels) 
if force or not exists(join(output_dir,cortical_dir,"rh.temporalpole.label")):
    system(join(fs_dir,"bin",'mri_annotation2label') + " --subject %s --hemi rh --annotation aparc --outdir %s" % (subject,join(output_dir,cortical_dir)))

if force or not exists(join(output_dir,cortical_dir,"lh.temporalpole.label")):
    system(join(fs_dir,"bin",'mri_annotation2label') + " --subject %s --hemi lh --annotation aparc --outdir %s" % (subject,join(output_dir,cortical_dir)))
        
if not exists(join(output_dir,vol_dir)):
    mkdir(join(output_dir,vol_dir))

# extract volume labels (label2vol)
for label in glob(join(output_dir,cortical_dir,"*.label")):
    vol_name = splitext(split(label)[1])[0] + ".nii.gz"
        
        
    if force or not exists(join(output_dir,vol_dir,vol_name)):
        system(join(fs_dir,"bin",'mri_label2vol') + " --label %s --temp %s --identity --o %s" % (label,T1,join(output_dir,vol_dir,vol_name)))
    
# make_subcortical_vols
if force or not exists(join(output_dir,"aseg.nii.gz")):
    system(join(fs_dir,'bin/mri_convert') + " %s %s" % (join(output_dir,"mri","aseg.mgz"),join(output_dir,"aseg.nii.gz")))
    
if not exists(join(output_dir,sub_vol_dir)):
    mkdir(join(output_dir,sub_vol_dir))
    
    
if force or not exists(join(output_dir,sub_vol_dir,"lh_acumbens.nii.gz")):    
    indices = join(split(abspath(argv[0]))[0],"subcortical_index")
    
    for line in open(indices,"r").readlines():
        num = line.split(":")[0].lstrip().rstrip()
        area = line.split(":")[1].lstrip().rstrip()
        print "Processing ", area+".nii.gz" 
        system(join(fsl,'bin/fslmaths') + " %s -uthr %s -thr %s -bin %s" % (join(output_dir,"aseg.nii.gz"),num,num,
                                                                            join(output_dir,sub_vol_dir,area+".nii.gz")))

vol_dir_out = vol_dir + "_s2fa"
if not exists(join(output_dir,vol_dir_out)):
    mkdir(join(output_dir,vol_dir_out))
if force or not exists(join(output_dir,vol_dir_out,"rh.bankssts_sf2.nii.gz")):     
    for volume in glob(join(output_dir,vol_dir,"*.nii.gz")):
        name = splitext(splitext(split(volume)[1])[0])[0] + "_sf2.nii.gz"
        out_vol = join(output_dir,vol_dir_out,name)
        print "Processing ", split(volume)[1], " -> ", split(out_vol)[1]
        system(join(fsl,'bin/flirt') + " -in %s -ref %s -out %s  -applyxfm -init %s" % (volume,join(output_dir,"FA.nii.gz"),
                                                                                    out_vol,join(output_dir,"T12FA.mat")))
        system(join(fsl,'bin/fslmaths') + " %s -thr %s -bin %s " % (out_vol,threshold,out_vol))


vol_dir_out = sub_vol_dir + "_s2fa"
if not exists(join(output_dir,vol_dir_out)):
    mkdir(join(output_dir,vol_dir_out))
if force or not exists(join(output_dir,vol_dir_out,"lh_acumbens_s2f.nii.gz")):     
    for volume in glob(join(output_dir,sub_vol_dir,"*.nii.gz")):
        out_vol = join(output_dir,vol_dir_out,splitext(splitext(split(volume)[1])[0])[0] + "_sf2.nii.gz")
        print "Processing ", split(volume)[1], " -> ", split(out_vol)[1]
        system(join(fsl,'bin/flirt') + " -in %s -ref %s -out %s  -applyxfm -init %s" % (volume,join(output_dir,"FA.nii.gz"),
                                                                                        out_vol,join(output_dir,"T12FA.mat")))
        system(join(fsl,'bin/fslmaths') + " %s -thr %s -bin %s " % (out_vol,threshold,out_vol))
        

if not exists(join(output_dir,"EDI")):
    mkdir(join(output_dir,"EDI"))

if not exists(join(output_dir,"EDI","allvols")):
    mkdir(join(output_dir,"EDI","allvols"))

############
### Need to implement makeseed script

exit(0)

if force or not exists(join(output_dir,"EDI","allvols","rh_thalamus_s2fa.nii.gz")):
    for files in glob(join(output_dir,"volumes_cortical_s2fa","*")):
        copyfile(files, join(output_dir,"EDI","allvols"))
    
    for files in glob(join(output_dir,"volumes_subcortical_s2fa","*")):
        copyfile(files, join(output_dir,"EDI","allvols"))

if exists(join(output_dir,"bs.nii.gz")):
    copyfile(join(output_dir,"bs.nii.gz"),join(output_dir,"EDI"))
    copyfile(join(output_dir,"terminationmask.nii.gz"),join(output_dir,"EDI"))
    copyfile(join(output_dir,"exlusion_bsplusthalami.nii.gz"),join(output_dir,"EDI"))
    copyfile(join(output_dir,"allvoxelscortsubcort.nii.gz"),join(output_dir,"EDI"))
else:
    copyfile(join(output_dir,"terminationmask.nii.gz"),join(output_dir,"EDI"))
       
    
