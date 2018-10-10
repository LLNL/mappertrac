from os import system,mkdir,remove,environ
from os.path import exists,join,split,splitext,abspath
from shutil import *
from glob import glob
from subscripts.utilities import *

def maskseeds(root_dir,input_dir,output_dir,low_threshold,high_threshold,high_threshold_thalamus,force=False):
    
    fsl = environ['FSL_DIR']

    print(fsl)
    
    if force and exists(output_dir):
        rmtree(output_dir)
        
    
    if not exists(output_dir):
        mkdir(output_dir)
        
    # Now create two transformed volumes with threshold 1 and 2
    run("fslmaths {} -thr {} -uthr {} -bin tmp.nii.gz".format(join(root_dir,"FA.nii.gz"),low_threshold,high_threshold))
    run("fslmaths {} -thr {} -uthr {} -bin tmp_thalamus.nii.gz".format(join(root_dir,"FA.nii.gz"),low_threshold,high_threshold_thalamus))
    
    for seed in glob(join(input_dir,"*s2fa.nii.gz")):

        region = split(seed)[1].split(".")[1].split("_")[0]
    
        if force or not exists(join(output_dir,split(seed)[1])):           
            if region == "thalamus":
                run("fslmaths {} -mas tmp_thalamus.nii.gz {}".format(seed, join(output_dir,split(seed)[1])))
            else:
                run("fslmaths {} -mas tmp.nii.gz {}".format(seed, join(output_dir,split(seed)[1])))
           
        
        
    smart_remove("tmp_thalamus.nii.gz")
    smart_remove("tmp.nii.gz")
    
    
def saveallvoxels(root_dir,cortical_dir,subcortical_dir,output_name,force):
    
    fsl = environ['FSL_DIR']

    if force:
        smart_remove(join(root_dir,"cort.nii.gz"))
        smart_remove(join(root_dir,"subcort.nii.gz"))
    
    all_vols = ""
    for vol in glob(join(cortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    

    #exit(0)

    if force or not exists(join(root_dir,"cort.nii.gz")):
        run("find_the_biggest {} {}".format(all_vols,join(root_dir,"cort.nii.gz")))
        
    all_vols = ""
    for vol in glob(join(subcortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    
    if force or not exists(join(root_dir,"subcort.nii.gz")):
        run("find_the_biggest {} {}".format(all_vols,join(root_dir,"subcort.nii.gz")))
    
    if force or not exists(output_name):
        run("fslmaths {} -add {} {} ".format(join(root_dir,"cort.nii.gz"),join(root_dir,"subcort.nii.gz"),output_name))
        run("fslmaths {} -bin {}".format(output_name,output_name))
    
