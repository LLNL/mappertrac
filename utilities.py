import time
from os import system,environ
from os.path import join


command_map = {
    'fslmaths' : join(environ['FSLDIR'],"bin","fslmaths"),
    'probtrackx2' : join(environ['FSLDIR'],"bin","probtrackx2"),
    'bedpostx_gpu' : join(environ['FSLDIR'],"bin",'bedpostx_gpu'),
    'bedpostx' : join(environ['FSLDIR'],"bin",'bedpostx')

    }


def run(exe,arguments):
    
    start = time.time()
    system(command_map[exe] + " " + arguments)
    stop = time.time()
    
    print "Running %s took %d seconds" % (exe, round(stop-start))
    print "\t %s %s" % (exe,arguments)
    
