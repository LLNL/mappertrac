import time
from os import system
from os.path import join,environ


command_map = {
    'fslmaths' : join(environ['FSL_DIR'],"bin","fslmaths"),
    'probtrackx2' : join(environ['FREESURFER_HOME'],"bin","probtrackx2"),
    }


def run(exe,arguments):
    
    start = time.time()
    system(command_map(exe) + " " + arguments)
    stop = time.time()
    
    print "Running %s took %d seconds" % (exe, round(stop-start))
    print "\t %s %s" % (exe,arguments)
    