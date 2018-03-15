from os import system,mkdir,remove,environ
from os.path import exists,join,split,splitext,abspath
from shutil import *
from glob import glob


def maskseeds(root_dir,input_dir,output_dir,low_threshold,high_threshold,high_threshold_thalamus,force=False):
    
    fsl = environ['FSL_DIR']

    print fsl
    
    if force and exists(output_dir):
        rmtree(output_dir)
        
    
    if not exists(output_dir):
        mkdir(output_dir)
        
    # Now create two transformed volumes with threshold 1 and 2
    print "fslmath %s -thr %f -uthr %f -bin tmp.nii.gz" % (join(root_dir,"FA.nii.gz"),low_threshold,high_threshold)
    system("time " + join(fsl,"bin","fslmaths") + " %s -thr %f -uthr %f -bin tmp.nii.gz" % (join(root_dir,"FA.nii.gz"),low_threshold,high_threshold))
    system("time " + join(fsl,"bin","fslmaths") + " %s -thr %f -uthr %f -bin tmp_thalamus.nii.gz" % (join(root_dir,"FA.nii.gz"),low_threshold,high_threshold_thalamus))
    
    for seed in glob(join(input_dir,"*s2fa.nii.gz")):

        region = split(seed)[1].split(".")[1].split("_")[0]
    
        if force or not exists(join(output_dir,split(seed)[1])):           
            if region == "thalamus":
                print "fslmaths  %s -mas tmp_thalamus.nii.gz %s" % (seed, join(output_dir,split(seed)[1]))
                system("time " + join(fsl,"bin","fslmaths") + " %s -mas tmp_thalamus.nii.gz %s" % (seed, join(output_dir,split(seed)[1])))
            else:
                print "fslmaths  %s -mas tmp.nii.gz %s" % (seed, join(output_dir,split(seed)[1]))
                system("time " + join(fsl,"bin","fslmaths") + " %s -mas tmp.nii.gz %s" % (seed, join(output_dir,split(seed)[1])))
           
        
        
    remove("tmp_thalamus.nii.gz")
    remove("tmp.nii.gz")
    
    
def saveallvoxels(root_dir,cortical_dir,subcortical_dir,output_name,force):
    
    fsl = environ['FSL_DIR']

    if force:
        remove(join(root_dir,"cort.nii.gz"))
        remove(join(root_dir,"subcort.nii.gz"))
    
    all_vols = ""
    for vol in glob(join(cortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    
    print join(fsl,"bin","find_the_biggest") + " %s %s" % (all_vols,join(root_dir,"cort.nii.gz"))

    #exit(0)

    if force or not exists(join(root_dir,"cort.nii.gz")):
        system("time " + join(fsl,"bin","find_the_biggest") + " %s %s" % (all_vols,join(root_dir,"cort.nii.gz")))
        
    all_vols = ""
    for vol in glob(join(subcortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    
    if force or not exists(join(root_dir,"subcort.nii.gz")):
        system("time " + join(fsl,"bin","find_the_biggest") + " %s %s" % (all_vols,join(root_dir,"subcort.nii.gz")))
    
    if force or not exists(output_name):
        system("time " + join(fsl,"bin","fslmaths") + " %s -add %s %s " % (join(root_dir,"cort.nii.gz"),join(root_dir,"subcort.nii.gz"),output_name))
        system("time " + join(fsl,"bin","fslmaths") + " %s -bin %s" % (output_name,output_name))
    
