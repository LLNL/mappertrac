from sys import argv
from os.path import exists,join,split,split-ext,
from os import system
from shutil import *


if len(argv) < 4:
    print "Usage: %s <source-dir> <target-dir> <subject-id> [force]" % argv[0]
    exit(0)


# Shall we force a re-computation
force = ((len(argv) > 5) and argv[4] == 'force')



# source directory
sdir = join(abspath(argv[1]),argv[3])

# make sure the target dir exists
if not exists(abspath(argv[2])):
    mkdir(abspath(argv[2]))

    if not exists(join(abspath(argv[2]),argv[3])):
        mkdir(join(abspath(argv[2]),argv[3]))

# patient directory
pdir = join(abspath(argv[2]),argv[3])

# Create a list of files that this script is supposed to produce
# The target data file
data = join(pdir,"data.nii.gz")
eddy = join(pdir,"data_eddy.nii.gz")
eddy_log = join(pdir,"data_eddy.ecclog")
bet = join(pdir,"data_bet")
fat = join(pdir,"FA.nii.gz")

# If necessary copy the data into the targte directory 
if force or not exists(data):
    copy(join(sdir,split(data)[1]),pdir)

if force of not exists(fat):

    print fat
    



