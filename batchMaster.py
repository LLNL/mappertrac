#!/usr/bin/env python

import os
from os.path import split,splitext,basename,isfile,abspath,join,exists
import string
import sys
import struct
import time
import stat
import glob
import socket
import grp

src_dir = split(abspath(sys.argv[0]))[0]

mpi_wrapper = join(src_dir,"mpiWrapper.c")
mpi_split = join(src_dir,"mpiSplitCalls.py")

cmd_file_header = """#!/usr/bin/env python
from os import system

"""

nersc_systems = ['carver','hopper','euclid','franklin']
ornl_systems  = ['titan','lens','ewok']
utah_systems  = ['uv']
anl_systems = ['intrepid','eureka']
llnl_systems = ['quartz','cab','surface','syrah','pascal']

machine_name = 'unknown'
if "NERSC_HOST" in os.environ:
    machine_name = os.environ['NERSC_HOST']

if machine_name == 'unknown' and "HOSTNAME" in os.environ:

    if os.environ['HOSTNAME'].startswith('lens'):
        machine_name = 'lens'
        queue = 'comp'
    elif  os.environ['HOSTNAME'].startswith('titan'):
        machine_name = 'titan'
        queue = 'batch'
    elif os.environ['HOSTNAME'] == "uv":
        machine_name = 'uv'
    elif os.environ['HOSTNAME'].startswith('cab'):
        machine_name = 'cab'
    elif os.environ['HOSTNAME'].startswith('surface'):
        machine_name = 'surface'
    elif os.environ['HOSTNAME'].startswith('syrah'):
        machine_name = 'syrah'
    elif os.environ['HOSTNAME'].startswith('quartz'):
        machine_name = 'quartz'
    elif os.environ['HOSTNAME'].startswith('pascal'):
        machine_name = 'pascal'

if machine_name == 'unknown' and ('intrepid' in os.environ['PS1']):
    machine_name = 'intrepid'
elif machine_name == 'unknown' and ('eureka' in os.environ['PS1']):
    machine_name = 'eureka'


print "Machine: ", machine_name

if machine_name == 'unknown':
    print "Machine not recognized ... sorry."
    exit(0)

if machine_name in nersc_systems:
    project = "m636"
    queue = "regular"
elif machine_name in ornl_systems:

    # If we are a member of the VACET bank
    if os.getlogin() in grp.getgrnam('csc025')[-1]:
        project = "CSC025EWK" # That is the bank we should be using    
    else:
        print "Could not determine your compute bank ... sorry."
        exit(0)
elif machine_name in anl_systems:
    project = 'SDAV'
    queue = 'prod'
elif machine_name in llnl_systems:
    project = 'asccasc'
    queue = 'pbatch'
else:
    queue = 'debug'
    project = 'noWork'
    
if machine_name == 'carver' or machine_name == 'lens' or machine_name == 'cab' or machine_name == 'surface' or machine_name == 'syrah' or machine_name == 'quartz' or machine_name == 'pascal':
    mpicc = "mpicc"
elif machine_name == 'hopper' or machine_name == "franklin" or machine_name == "titan":
    mpicc = "cc"
elif machine_name in ['euclid','uv']  :
    mpicc = "gcc -lmpi "
elif machine_name == 'intrepid':
    mpicc = 'mpixlc'
elif machine_name == 'eureka':
    mpicc = 'gcc'


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
cores_per_job = int(sys.argv[next_arg+1])

runtime = int(sys.argv[next_arg+2])
cmdName = basename(splitext(sys.argv[next_arg+3])[0])

# if necessary create a subdirectory ./qsub to store the .qsub and log files
psub = join(os.getcwd(),"qsub")
if not os.path.exists(psub) :
    os.mkdir(psub)

mpi_cmd_file = join(psub,"mpiCommandLines_%02d.cmd")

# Search for a free cmd-file name  
fileCount = 0
while isfile(mpi_cmd_file % fileCount):
    fileCount += 1
mpi_cmd_file = mpi_cmd_file % fileCount
log_file = join(psub,"Log_%s_%02d.cmd" % (cmdName,fileCount))

# Now compile the mpiWrapper in the qsub directory so we are sure that it is
# compiled for the current machine
os.system("%s -o %s %s" % (mpicc,join(psub,"mpiWrapper"),mpi_wrapper))
mpi_wrapper = join(psub,"mpiWrapper")

# Now assemble all the command lines that this call expands to
command_lines = []

if have_range:
    
    for i in range(start,stop,sub_step):
        cmd = ""
        
        for arg in sys.argv[next_arg+3:] : #The rest of the command-line
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
        
        for arg in sys.argv[next_arg+3:] : #The rest of the command-line
            if arg == "--input":
                cmd += " " + file
            else:

                if exists(arg):
                    cmd += " " + abspath(arg)
                else:
                    cmd += " " + arg

        command_lines.append(cmd)
        
    print "Found %d command lines" % len(command_lines)
if nr_of_jobs > len(command_lines):
    nr_of_jobs = len(command_lines)

# Divide all command lines into nr_of_jobs many pieces
batch_size = len(command_lines) / nr_of_jobs
if batch_size * nr_of_jobs < len(command_lines):
    batch_size += 1

# Now that we determined the number of jobs run on each processor we can
# determine the actual run-time
runtime *= batch_size

# Finally we determine the node_count, mppwidth, and the mpirun call for the system
if machine_name == 'carver':
    node_count = nr_of_jobs * cores_per_job / 8
    if node_count*8 < nr_of_jobs * cores_per_job:
        node_count += 1
    pbs_width = "nodes=%d" % node_count
    mpi_run = 'mpirun -np'
elif machine_name == 'lens':
    node_count = nr_of_jobs * cores_per_job / 16
    if node_count*16 < nr_of_jobs * cores_per_job:
        node_count += 1
    pbs_width = "nodes=%d:ppn=%d" % (node_count,min(nr_of_jobs*cores_per_job,16))
    mpi_run = 'mpirun -np'
elif machine_name == 'titan':
    mppwidth = nr_of_jobs * cores_per_job
    # Request must be a multiple of 16
    if mppwidth % 16 != 0:
        mppwidth  = (mppwidth / 16 + 1)*16
        
    pbs_width = "nodes=%d" % (mppwidth/16)
    mpi_run = 'aprun -n '
elif machine_name == 'hopper':
    mppwidth = nr_of_jobs * cores_per_job
    if mppwidth % 24 != 0:
        mppwidth  = (mppwidth / 24 + 1) * 24
    pbs_width = "mppwidth=%d" % mppwidth
    mpi_run = 'aprun -a xt -N %d -n ' % (24 / cores_per_job)
elif machine_name == 'franklin':
    mppwidth = nr_of_jobs * cores_per_job
    pbs_width = "mppwidth=%d" % mppwidth
    mpi_run = 'aprun -n'
elif machine_name == 'euclid' or machine_name == 'uv':
    node_count = nr_of_jobs * cores_per_job
    pbs_width = "nodes=%d" % node_count
    mpi_run = 'mpirun -np'
elif machine_name == 'uv':
    node_count = nr_of_jobs * cores_per_job
    pbs_width = "nodes=%d" % node_count
    mpi_run = 'mpirun -np'
elif machine_name == 'intrepid':
    node_count = nr_of_jobs * cores_per_job / 4
    if node_count*4 < nr_of_jobs * cores_per_job:
        node_count += 1
    if node_count < 512: # intrepid doesn't give out fewer nodes
        node_count = 512
    
elif machine_name == 'eureka':
    node_count = nr_of_jobs * cores_per_job / 8
    if node_count*8 < nr_of_jobs * cores_per_job:
        node_count += 1
    
elif machine_name == 'cab' or machine_name == 'surface' or machine_name == 'syrah':                                                                                                          
    node_count = nr_of_jobs * cores_per_job / 16
    if node_count*16< nr_of_jobs * cores_per_job:
        node_count += 1 
       
    pbs_width = "nodes=%d" % node_count                                                                                               
    mpi_run = "srun -n"                                                                                                            

elif machine_name == 'cab' or machine_name == 'surface' or machine_name == 'syrah':                                                             
    node_count = nr_of_jobs * cores_per_job / 16
    if node_count*16< nr_of_jobs * cores_per_job:
        node_count += 1

    pbs_width = "nodes=%d" % node_count
    mpi_run = "srun -n"

elif machine_name == 'quartz' or machine_name == 'pascal':
    node_count = nr_of_jobs * cores_per_job / 18
    if node_count*18< nr_of_jobs * cores_per_job:
        node_count += 1

    pbs_width = "nodes=%d" % node_count
    mpi_run = "srun -n"


else:
    print "System %s not recognized ... can't create scripts" % machine_name
    sys.exit(0)
    

    

mpi_cmd = open(mpi_cmd_file,"w")
os.chmod(mpi_cmd_file,stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)
for i in xrange(0,len(command_lines),batch_size):

    # Search for a free cmd-file name  
    fileCount = 0
    cmdFile = join(psub,"cmd-%02d-%02d_%02d.py" % (i,i+batch_size,fileCount))
    while isfile(cmdFile):
        fileCount += 1
        cmdFile = join(psub,"cmd-%02d-%02d_%02d.py" % (i,i+batch_size,fileCount))
    
    mpi_cmd.write("%s\n" % cmdFile)

    file = open(cmdFile,"w")
    file.write(cmd_file_header)
    for cmd in command_lines[i:i+batch_size]:
        file.write("system(\"%s\")\n" % cmd)
    file.close()
    os.chmod(cmdFile,stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)


mpi_cmd.close()


# Search for a free .qsub name to write this script into
fileCount = 0
script_name = join(psub,"%s_%02d.qsub" % (cmdName,fileCount))
while isfile(script_name) :
    fileCount += 1
    script_name = join(psub,"%s_%02d.qsub" % (cmdName,fileCount))

            
script_header = """#!/bin/sh
#PBS -N %s
#PBS -o %s                                                                                                                            
#PBS -j oe
#PBS -l walltime=%02d:%02d:00                                                                                                         
#PBS -l %s                                                                                                                            
#PBS -A %s                                                                                                                            
#PBS -q %s                                                                                                                            
cd $PBS_O_WORKDIR   # change directory to where script was submitted 
date; 
"""
script_llnl = """#!/bin/sh
#MSUB -N %s
#MSUB -o %s
#MSUB -j oe
#MSUB -l walltime=%02d:%02d:00
#MSUB -l %s
#MSUB -A %s
#MSUB -q %s
date; 
"""

# Create the qsub-script
if machine_name not in anl_systems:

    if machine_name not in llnl_systems:
        script = script_all % (cmdName,log_file,runtime / 60, runtime % 60, pbs_width, project, queue)
    else:
        script = script_llnl % (cmdName,log_file,runtime / 60, runtime % 60, pbs_width, project, queue)

    if machine_name == 'hopper':
        script += "\nexport CRAY_ROOTFS=DSL\n\n"

    if nr_of_jobs > cores_per_job:
        script += "%s %d %s %s %s\n" % (mpi_run,nr_of_jobs,mpi_wrapper,mpi_split,mpi_cmd_file)
    else:
        for i in xrange(0,len(command_lines),nr_of_jobs):
            for cmd in command_lines[i:i+nr_of_jobs]:
                script += "\n%s %d %s &" % (mpi_run,cores_per_job,cmd)
            script += "\nwait\n"


elif machine_name == 'intrepid':

    script = "#!/bin/sh\n"
    script += "qsub --mode vn -A %s -q %s -t %d -e %s -o %s -n %d --proccount %d %s %s %s" % (
        project,queue, runtime, log_file,log_file, node_count, nr_of_jobs,mpi_wrapper, mpi_split,mpi_cmd_file)

elif machine_name == 'eureka':

    script = "#!/bin/sh\n"
    script += "qsub -A %s -t %d -e %s -o %s -n %d " % (project, runtime, log_file,log_file, node_count)
    script += "--mode script %s\n" % (splitext(script_name)[0] + ".sh")
   
    run_script = "#!/bin/sh\n\n"
    run_script += "mpdboot -n %d -f $COBALT_NODEFILE\n" % node_count
    run_script += "/soft/apps/mpich2-1.0.8p1/bin/mpirun -np %d %s %s %s" % (nr_of_jobs*cores_per_job,mpi_wrapper, mpi_split,mpi_cmd_file)


# write the script
print "Creating %s\n"% script_name
f = open(script_name, "w")
f.write(script)
f.close()

if machine_name in anl_systems:
    name = splitext(script_name)[0] + ".sh"
    f = open(name,"w")
    f.write(run_script)
    f.close()
    os.system("chmod +x %s" % name)

print join(psub, script_name)

#sys.exit(0)
# submit it to the batch system
# os.system('qsub %s'%name)

   
