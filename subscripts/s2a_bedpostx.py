#!/usr/bin/env python3
from s_all_parsl import executor_labels
from subscripts.utilities import run,smart_mkdir,smart_run,exist_all
from os.path import join,exists,basename
from shutil import copyfile,rmtree

@python_app(executors=executor_labels)
def s2a_bedpostx(sdir, stdout):
    bedpostx = join(sdir,"bedpostx_b1000")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    th1 = join(bedpostxResults, "merged_th1samples")
    ph1 = join(bedpostxResults, "merged_ph1samples")
    th2 = join(bedpostxResults, "merged_th2samples")
    ph2 = join(bedpostxResults, "merged_ph2samples")
    dyads1 = join(bedpostxResults, "dyads1")
    dyads2 = join(bedpostxResults, "dyads2")
    brain_mask = join(bedpostxResults, "nodif_brain_mask")
    if exists(bedpostxResults):
        rmtree(bedpostxResults)
    smart_mkdir(bedpostx)
    smart_mkdir(bedpostxResults)
    copyfile(join(sdir,"data_eddy.nii.gz"),join(bedpostx,"data.nii.gz"))
    copyfile(join(sdir,"data_bet_mask.nii.gz"),join(bedpostx,"nodif_brain_mask.nii.gz"))
    copyfile(join(sdir,"bvals"),join(bedpostx,"bvals"))
    copyfile(join(sdir,"bvecs"),join(bedpostx,"bvecs"))

    run("bedpostx_gpu " + bedpostx + " -NJOBS 4", stdout)
    run("make_dyadic_vectors {} {} {} {}".format(th1,ph1,brain_mask,dyads1), stdout)
    run("make_dyadic_vectors {} {} {} {}".format(th2,ph2,brain_mask,dyads2), stdout)

def create_job(sdir, stdout, checksum):
    return s2a_bedpostx(sdir, stdout)