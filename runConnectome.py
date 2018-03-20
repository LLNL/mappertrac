from sys import argv
from os.path import abspath,join,split
from os import system

if len(argv) < 4:
    
    print """
    Usage: %s <machine-config.sh> <source-dir> <output-dir> [<subject-name>]
    
    Example: %s surface-config.sh ./imaging/TRACKPilotNiiData/GO-TBI13 ./processed
    This will process the data of GO-TBI13 and save the results in ./processed/GO-TBI13
    
    If the optional <subject-name> is given the output directory will be renamed
    ./processed/<subject-name>
    """ % (argv[0],argv[0])
    
    exit(0) 
       
path = split(abspath(argv[0]))[0]      
config = abspath(argv[1])
source = abspath(argv[2])
target = abspath(argv[3])

if len(argv) > 4:
    output_dir = join(target,argv[4])
else:
    output_dir = join(target,split(source)[1]) 

# Make sure that the correct environement variables are set    
system("source " + config)

system("python " + join(path,"dti_preproc") + " " + source + " " + target)
system("python " + join(path,"bedpostx") + " " + target)
system("python " + join(path,"freesurfer_preproc") + " " + target)
system("python " + join(path,"prepTract") + " " + target)




      
