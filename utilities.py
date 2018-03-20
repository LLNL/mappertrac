import time
from os import system,environ
from os.path import join


command_map = {
    'fslmaths' : join(environ['FSLDIR'],"bin","fslmaths"),
    'probtrackx2' : join(environ['FSLDIR'],"bin","probtrackx2"),
    'bedpostx_gpu' : join(environ['FSLDIR'],"bin",'bedpostx_gpu'),
    'bedpostx' : join(environ['FSLDIR'],"bin",'bedpostx'),
    'bet' : join(environ['FSLDIR'],"bin",'bet'),
    'fdt_rotate_bvecs' : join(environ['FSLDIR'],"bin",'fdt_rotate_bvecs'),
    'flirt' : join(environ['FSLDIR'],"bin",'flirt'),
    'convert_xfm' : join(environ['FSLDIR'],"bin",'convert_xfm'),
    'find_the_biggest' : join(environ['FSLDIR'],"bin",'find_the_biggest'),
    'make_dyadic_vectors' : join(environ['FSLDIR'],"bin",'make_dyadic_vectors'),
    'eddy_correct' : join(environ['FSLDIR'],"bin",'eddy_correct'),
    'dtifit' : join(environ['FSLDIR'],"bin",'dtifit'),



    'mri_convert' : join(environ['FREESURFER_HOME'],"bin",'mri_convert'),
    'recon-all' : join(environ['FREESURFER_HOME'],"bin",'recon-all'),
    'mri_annotation2label' : join(environ['FREESURFER_HOME'],"bin",'mri_annotation2label'),
    'mri_label2vol' : join(environ['FREESURFER_HOME'],"bin",'mri_label2vol'),

    }


def run(exe,arguments):
    
    start = time.time()
    system(command_map[exe] + " " + arguments)
    stop = time.time()
    
    print "Running %s took %d seconds" % (exe, round(stop-start))
    print "\t %s %s" % (exe,arguments)
    
