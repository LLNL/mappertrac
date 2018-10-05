#!/usr/bin/env python

import os
from os.path import split,splitext,basename,isfile,abspath,join,exists
import string
import sys
import struct
import time
import stat
import glob

src_dir = split(abspath(sys.argv[0]))[0]

project = 'ccp'
queue = 'pbatch'
mpicc = "mpicc"

if len(sys.argv) == 1 :
    print("""
    Usage:\n\t %s [<file-template> | <arguments.list | <start> <stop> <sub-step> ] <jobs> <cores> <max time> batchCommand\n
         where batchCommand can be any number of strings.

    This script creates single node batch jobs run via an mpi call.

    All files matching the template or the range [start,stop) (see below) are split into
    chunks of <ndumps> many files and define the input files for each job as described
    below. The script decides between the template and range case by testing wether
    the first argument can be converted into an integer. 

    ARGUMENTS:

         <file-template> a file name or template of file names. When
			 using a template it must be quoted to avoid pre
			 mature shell expansion

         <arguments.list> a file name with a list of arguments 

         <start>         beginning of the range
         <stop>          end of the range
         <sub-step>      step size 
         
         <jobs>          number of parallel jobs to start.  This implicitly
                         breaks the job into chunks and allows you to specify
                         --input (see below) as argument to the batchCommand
                         defining the file(s) on which a particular batch job
                         will operate.

         <cores>         the number of cores each job uses. This allows the script to
                         allocated the correct number of jobs per node and/or the correct
                         mppwidth. Note that the basic mpi-script still only uses <ndumps>
                         many jobs.

         <max time>      maximum time alloted for each single job. Note, that on many
                         machines queues are limited in the number of hours jobs can last.
                         Thust you should check the resulting time of the complete job

         For example, for <file-template>=plt050%%.tar and <ndumps> 5 the script
         will create two batch jobs one processing files plt0500-plt504 and the
         other one processing files plt0505-plt509
         
    SPECIAL BATCH COMMAND ARGUMENTS:

    The following keywords can be supplied on the 
    command line and are converted into arguments for the 
    batchCommand as follows:

       --input    in the case of a template is replaced by an
                  absolute file name.

       --index    in the range case is replaced by an integer

    The first string of batchcommand (the command itself) will 
    automatically be converted into an absolute path.
    """ % (sys.argv[0])) 
    sys.exit()

have_range = True
try:
    int(sys.argv[1])
except:
    have_range = False

if have_range:
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    sub_step = int(sys.argv[3])
    next_arg = 4
else:  
    template = sys.argv[1]
    sub_step = 1
    next_arg = 2

nr_of_jobs = int(sys.argv[next_arg])

runtime = int(sys.argv[next_arg+1])
cmdName = basename(splitext(sys.argv[next_arg+2])[0])
scriptName = basename(splitext(sys.argv[next_arg+3])[0])

# if necessary create a subdirectory ./qsub to store the .qsub and log files
psub = join(os.getcwd(),"qsub")
if not os.path.exists(psub) :
    os.mkdir(psub)


# Now assemble all the command lines that this call expands to
command_lines = []

if have_range:
    
    for i in range(start,stop,sub_step):
        cmd = ""
        
        for arg in sys.argv[next_arg+2:] : #The rest of the command-line
            if arg == "--index":
                cmd += " %d" % i
            elif arg == "--next":
                cmd += " %d " % (i+sub_step)
            elif '%d' in arg or '%01d' in arg or '%02d' in arg or '%03d' in arg or '%04d' in arg:
                cmd += " " + arg % i
            else:
                if exists(arg):
                    cmd += " " + abspath(arg)
                else:
                    cmd += " " + arg

        command_lines.append(cmd)
    
else:

    print "Template ", splitext(template)[1]
    if splitext(template)[1] == ".list":

        # We abuse the files functionality from below and
        inputs = open(template,"r")
        files = [line.rstrip().lstrip() for line in inputs.readlines()]
    else:
        # First assume we got a file template
        files = sorted(glob.glob(template))
        if len(files) == 0: # If we did not get files we assume flat inputs
            files = template.split()
        else:
            files = [abspath(f) for f in files]

    for file in files:

        cmd = ""
        
        for arg in sys.argv[next_arg+2:] : #The rest of the command-line
            if arg == "--input":
                cmd += " " + file
            else:

                if exists(arg):
                    cmd += " " + abspath(arg)
                else:
                    cmd += " " + arg

        command_lines.append(cmd)
        
    print "Found %d command lines" % len(command_lines)

# Search for a free .qsub name to write this script into
fileCount = 0
script_name = join(psub,"%s_%02d.qsub" % (scriptName,fileCount))
log_file = join(psub,"log_%s_%02d.stdout" % (scriptName,fileCount))
while isfile(script_name) :
    fileCount += 1
    script_name = join(psub,"%s_%02d.qsub" % (scriptName,fileCount))
    log_file = join(psub,"log_%s_%02d.stdout" % (scriptName,fileCount))

script_llnl = """#!/bin/sh
#MSUB -N %s
#MSUB -o %s
#MSUB -j oe
#MSUB -l walltime=%02d:%02d:00
#MSUB -l nodes=%s
#MSUB -A %s
#MSUB -q %s
date; 
"""

script = script_llnl % (cmdName,log_file,runtime / 60, runtime % 60, nr_of_jobs, project, queue)

for i in xrange(0,len(command_lines),nr_of_jobs):
    for cmd in command_lines[i:i+nr_of_jobs]:
        script += "\nsrun -n 1 -c 36%s &" % cmd
    script += "\nwait\n"

# write the script
print "Creating %s\n"% script_name
f = open(script_name, "w")
f.write(script)
f.close()

   
