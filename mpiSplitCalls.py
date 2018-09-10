#!/usr/bin/env python

from os import system,environ,popen
from os.path import isfile
from sys import argv
import sys


if len(argv) < 2:
    print """
    Usage %s <cmdFile> [<rank>]

    This script will take the cmdFile and execute the n-th line
    when MPIRUN_RANK = n
    """ % argv[0]
    sys.exit(0)


commands = []
cmd = ""

if not isfile(sys.argv[1]):
    print "Error could not open command file .. abort"
    sys.exit(0)

f = open(sys.argv[1],"r")



# if no explicit mpi rank was given we try to infer one from the environment
if len(argv) < 3:

    if 'MPIRUN_RANK' in environ:
        mpi_rank = int(environ['MPIRUN_RANK'])
    elif 'OMPI_MCA_ns_nds_vpid' in environ:
       mpi_rank = int(environ['OMPI_MCA_ns_nds_vpid'])
    else:
        # if we can't determine the rank we give up
        print "%s called outside an mpi loop ... abort\n" % argv[0]
        sys.exit(0)

else:
    # otherwise the second argument is supposed to be the rank
    mpi_rank = int(argv[2])
    

# parse all the commands
for line in f.readlines():

    commands.append(line)


#pipe = popen("env | grep MPI",'r')
#for line in pipe.readlines():
#	print line

#sys.exit(0)
#if rank != "":
#    rank = rank[:-1]

#print "Rank " , rank
#print commands
#sys.exit(0)


# if there are enough commands for our rank
if len(commands) > mpi_rank:
    # execute
    print commands[mpi_rank]
    system(commands[mpi_rank])

