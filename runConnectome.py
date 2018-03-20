from sys import argv
from os.path import abspath,join,split
from os import system

if len(argv) < 3:
    
    print """
    Usage: %s <source-dir> <output-dir> [<subject-name>]
    
    Example: %s ./imaging/TRACKPilotNiiData/GO-TBI13 ./processed
    This will process the data of GO-TBI13 and save the results in ./processed/GO-TBI13
    
    If the optional <subject-name> is given the output directory will be renamed
    ./processed/<subject-name>
    """ % (argv[0],argv[0])
    
    exit(0) 
       
path = split(abspath(argv[0]))[0]      
source = abspath(argv[1])
target = abspath(argv[2])

if len(argv) > 3:
    output_dir = join(target,argv[3])
else:
    output_dir = join(target,split(source)[1]) 


system("python " + join(path,"dti_preproc.py") + " " + source + " " + target)
system("python " + join(path,"bedpostx.py") + " " + target)
#system("python " + join(path,"freesurfer_preproc.py") + " " + target)
#system("python " + join(path,"prepTract.py") + " " + target)




      
