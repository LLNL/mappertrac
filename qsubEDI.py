from sys import argv
from os.path import *
from os import system
from glob import glob

if len(argv) < 6:
    print "Usage: %s PBSBatchMaster.py <jobs> <vol-dir> <bedpostX-dir> <output-dir>" % argv[0]
    exit(0)
    
# Assemble all files 
files = glob(join(argv[3],"*_s2fa.nii.gz"))
files = [abspath(f) for f in files]


# Make a temporary argument list
args = open("arguments.list","w")

for f1 in files:
    for f2 in files:
        if f1 != f2:
            args.write(f1 + ":" + f2 + "\n")

args.close()

pbs_cmd = ("python %s" % argv[1] 
           + " arguments.list %s 1 15" % argv[2]
           + " python %s" % join(split(argv[0])[0],"runEDI.py")
           + " --input"
           + " %s" % split(split(abspath(argv[3]))[0])[0]
           + " %s" % abspath(argv[4])
           + " %s" % abspath(argv[5]) 
           )

system(pbs_cmd)




