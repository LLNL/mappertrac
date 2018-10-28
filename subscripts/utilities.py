import time
import datetime
import os
import subprocess
import sys
import shutil
import hashlib
from glob import glob
from os.path import exists,join,split,splitext,abspath,basename,isdir
from os import system,environ,makedirs,remove
from subprocess import Popen,PIPE

def smart_mkdir(path):
    if not isdir(path):
        makedirs(path)
        run("chmod 777 {}".format(path))

def smart_remove(path):
    if isdir(path):
        shutil.rmtree(path)
    elif exists(path):
        remove(path)

def exist_all(paths):
    for path in paths:
        if not exists(path):
            return False
    return True

def run(command, stdout=None, container=None, ignore_errors=False, print_output=True, print_time=False, name_override="", working_dir=None):
    start = time.time()

    if container is not None:
        command = "singularity exec -B .:/share {} python3 /run.py {}".format(container, command)

    process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ, cwd=working_dir)
    line = ""
    while True:
        new_line = process.stdout.readline()
        new_line = str(new_line, 'utf-8')[:-1]
        if print_output and new_line:
            print(new_line)
        if stdout is not None and not new_line.isspace():
            write(stdout, new_line)
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
        if stdout is not None and not new_line.isspace():
            write(stdout, "Error: non zero return code")
            write(stdout, get_time_date())
        raise Exception("Non zero return code: {}".format(process.returncode))
    else:
        tokens = command.split(' ')
        if print_time:
            if name_override != "":
                tokens[0] = name_override
            print("Running {} took {} (h:m:s)".format(tokens[0], get_time_string(int(time.time()) - start)))
            if len(tokens) > 1:
                print("\tArgs: {}".format(' '.join(tokens[1:])))
    return line  # return the last output line

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def str2bool(string):
    if string is None:
        return string
    return string.lower() in ("yes", "true", "t", "1")

def get_time_date():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")

def get_time_string(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "{:02d}:{:02d}:{:02d}".format(int(h), int(m), int(s))

def get_time_seconds(string):
    while (len(string.split(":")) < 3):
        string = "00:" + string
    return sum(secs * int(digit) for secs,digit in zip([3600, 60, 1], string.split(":")))

def get_start(function_name=sys.argv[0]):
    return "Starting {} at {}\n".format(basename(str(function_name)), get_time_date())

def get_finish(function_name=sys.argv[0]):
    return "Finished {} at {}\n".format(basename(str(function_name)), get_time_date())

def print_time():
    print(get_time_date())

def print_start():
    print(get_start())
    return time.time()

def print_finish(start_time):
    print(get_finish())

def write(path, output):
    with open(path, 'a') as f:
        f.write(output + "\n")

def write_start(path, function_name=sys.argv[0]):
    with open(path, 'a') as f:
        f.write("\n=====================================\n")
        f.write(get_start(function_name))
        f.write("=====================================\n\n")
        f.write("record_time,{}\n".format(int(time.time())))

def write_finish(path, function_name=sys.argv[0]):
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
        f.write(get_finish(function_name))
        if is_integer(start_time):
            f.write("Took {} (h:m:s)\n".format(get_time_string(int(time.time()) - int(start_time))))
        f.write("=====================================\n\n")

def read_checkpoint(sdir, step, checksum):
    return read_checkpoints(sdir, [step], checksum)

def read_checkpoints(sdir, steps, checksum):
    odir, subject = split(sdir)
    checkpoints = join(odir, "checkpoints.txt")
    if exists(checkpoints):
        with open(checkpoints, 'r') as f:
            for line in f.readlines():
                chunks = line.strip().split(',')
                line_subject = chunks[0].strip()
                line_step = chunks[1].strip()
                line_checksum = chunks[2].strip()
                if len(chunks) < 4:
                    continue
                if line_subject == subject and line_step in steps and line_checksum == checksum:
                    steps.remove(line_step)
                if not steps:
                    return True
    return False

def write_checkpoint(sdir, step, checksum):
    odir, subject = split(sdir)
    with open(join(odir, "checkpoints.txt"), 'a') as f:
        f.write("{}, {}, {}, {}\n".format(subject, step, checksum, get_time_date()))

def generate_checksum(input_dir):
    buf_size = 65536  # read file in 64kb chunks
    md5 = hashlib.md5()
    for fname in ['anat.nii.gz','bvals','bvecs','hardi.nii.gz']:
        f = open(join(input_dir, fname), 'rb')
        while True:
            data = f.read(buf_size)
            if not data:
                break
            md5.update(data)
        f.close()
    return md5.hexdigest()

def generate_edge_list(vol_dir, path='lists/listEdgesEDIAll.txt'):
    with open(path,'w') as f:
        files = glob(join(vol_dir,"*_s2fa.nii.gz"))  # Assemble all files
        files = [abspath(vol) for vol in files]
        for a in files:
            for b in files:
                if a != b:
                    a1 = splitext(splitext(split(a)[1])[0])[0]
                    b1 = splitext(splitext(split(b)[1])[0])[0]
                    f.write("{},{}\n".format(basename(a1),basename(b1)))
