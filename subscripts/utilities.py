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

def smart_run(command, path, force=False):
    if not exists(path):
        run(command)

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
        if write_output != None:
            with open(write_output, 'a') as f:
                f.write("{}\n".format(new_line))
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
        printTime()
        raise Exception("Non zero return code: {}".format(process.returncode))
    else:
        tokens = command.split(' ')
        if output_time:
            if name_override != "":
                tokens[0] = name_override
            print("Running {} took {} seconds".format(tokens[0], round(time.time()-start)))
            if len(tokens) > 1:
                print("\tArgs: {}".format(' '.join(tokens[1:])))
    return line # return the last output line

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

def printTime():
    print("Time is {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")))

def printStart():
    print("Running {}".format(basename(sys.argv[0])))
    printTime()
    return time.time()

def printFinish(start_time):
    print("\n=====================================\n" +
          " {} took {} (h:m:s)".format(basename(sys.argv[0]), getTimeString(time.time() - start_time)) +
          "\n=====================================\n")
    printTime()

def getTimeString(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "{:d}:{:02d}:{:02d}".format(int(h), int(m), int(s))

def generateListAllEdges(vol_dir, path='lists/listEdgesEDIAll.txt'):
    with open(path,'w') as l:
        files = glob(join(vol_dir,"*_s2fa.nii.gz")) # Assemble all files 
        files = [abspath(f) for f in files]
        for a in files:
            for b in files:
                if a != b:
                    a1 = splitext(splitext(split(a)[1])[0])[0]
                    b1 = splitext(splitext(split(b)[1])[0])[0]
                    l.write("{},{}\n".format(basename(a1),basename(b1)))
