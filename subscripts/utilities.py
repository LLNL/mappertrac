import time
import datetime
import os
import subprocess
import sys
import shutil
from glob import glob
from os.path import exists,join,split,splitext,abspath,basename
from os import system,environ,makedirs,remove
from subprocess import Popen,PIPE


def smart_copy(src,dest,force=False):
    if force or not exists(dest):
        shutil.copyfile(src,dest)


def smart_mkdir(path):
    if not exists(path):
        makedirs(path)


def smart_run(command, path, force=False, write_output=None):
    if force or not exists(path):
        run(command, write_output=write_output)


def smart_remove(path):
    if exists(path):
        remove(path)


def exist_all(paths,ext=""):
    for path in paths:
        if not exists(path + ext):
            return False
    return True


def run(command, ignore_errors=False, print_output=True, output_time=False, name_override="", working_dir=None, write_output=None):
    start = time.time()
    process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ, cwd=working_dir)
    line = ""
    while True:
        new_line = process.stdout.readline()
        new_line = str(new_line, 'utf-8')[:-1]
        if print_output and new_line:
            print(new_line)
        if write_output is not None:
            with open(write_output, 'a') as f:
                f.write("{}\n".format(new_line))
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
        printTime()
        if write_output is not None and not new_line.isspace():
            writeOutput(write_output, "Error: non zero return code")
            writeOutput(write_output, getTimeDate())
        raise Exception("Non zero return code: {}".format(process.returncode))
    else:
        tokens = command.split(' ')
        if output_time:
            if name_override != "":
                tokens[0] = name_override
            print("Running {} took {} seconds".format(tokens[0], round(time.time() - start)))
            if len(tokens) > 1:
                print("\tArgs: {}".format(' '.join(tokens[1:])))
    return line  # return the last output line


def isFloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def isInteger(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def getTimeDate():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")


def getTimeDuration(start_seconds):
    duration = time.time() - int(start_seconds)
    m, s = divmod(duration, 60)
    h, m = divmod(m, 60)
    return "{:d}:{:02d}:{:02d}".format(int(h), int(m), int(s))


def getStart(function_name=sys.argv[0]):
    return "Starting {} at {}\n".format(basename(str(function_name)), getTimeDate())


def getFinish(function_name=sys.argv[0]):
    return "Finished {} at {}\n".format(basename(str(function_name)), getTimeDate())


def printTime():
    print(getTimeDate())


def printStart():
    print(getStart())
    return time.time()


def printFinish(start_time):
    print(getFinish())


def writeOutput(path, output):
    with open(path, 'a') as f:
        f.write(output + "\n")


def writeStart(path, function_name=sys.argv[0]):
    with open(path, 'a') as f:
        f.write("\n=====================================\n")
        f.write(getStart(function_name))
        f.write("=====================================\n\n")
        f.write("record_time,{}\n".format(int(time.time())))


# def writeStartCheckpoint():
#     with open(path, 'a') as f:
#         f.write("start_checkpoint,{}\n".format(int(time.time())))


# def writeEndCheckpoint():
#     with open(path, 'a') as f:
#         f.write("end_checkpoint,{}\n".format(int(time.time())))


# def getCheckpointTime(line):
#     chunks = line.split(',',1)
#     if len(chunks) > 1 and isInteger(chunks[1]):
#         return int(chunks[1])
#     else:
#         return 0


# def new_writeFinish(path, function_name=sys.argv[0]):
#     total_time = 0
#     start_time = 0
#     with open(path, 'r') as f:
#         for line in f.readlines():
#             line = line.strip()
#             if line.startswith("start_record"):
#                 total_time = 0
#             elif line.startswith("start_checkpoint"):
#                 start_time = getCheckpointTime(line)
#             elif line.startswith("end_checkpoint"):
#                 total_time += getCheckpointTime(line) - start_time


def writeFinish(path, function_name=sys.argv[0]):
    start_time = ""
    with open(path, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith("record_time,"):
                chunks = line.split(',',1)
                if len(chunks) > 1:
                    start_time = chunks[1]

    with open(path, 'a') as f:
        f.write("\n=====================================\n")
        f.write(getFinish(function_name))
        if isInteger(start_time):
            f.write("Took {} (h:m:s)\n".format(getTimeDuration(start_time)))
        f.write("=====================================\n\n")


def generateListAllEdges(vol_dir, path='lists/listEdgesEDIAll.txt'):
    with open(path,'w') as f:
        files = glob(join(vol_dir,"*_s2fa.nii.gz"))  # Assemble all files
        files = [abspath(vol) for vol in files]
        for a in files:
            for b in files:
                if a != b:
                    a1 = splitext(splitext(split(a)[1])[0])[0]
                    b1 = splitext(splitext(split(b)[1])[0])[0]
                    f.write("{},{}\n".format(basename(a1),basename(b1)))
