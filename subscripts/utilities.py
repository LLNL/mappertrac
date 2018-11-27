import time
import datetime
import os
import subprocess
import sys
import shutil
import hashlib
import grp
from glob import glob
from os.path import exists,join,split,splitext,abspath,basename,isdir
from os import system,environ,makedirs,remove
from subprocess import Popen,PIPE

def smart_mkdir(path):
    if not isdir(path):
        makedirs(path)

def smart_remove(path):
    """Remove all files and directories if they exist
    """
    if isdir(path):
        shutil.rmtree(path)
    elif exists(path):
        remove(path)

def exist_all(paths):
    for path in paths:
        if not exists(path):
            return False
    return True

def run(command, params=None, ignore_errors=False, print_output=True, print_time=False, working_dir=None):
    """Run a command in a subprocess.
    Safer than raw execution. Can also write to logs and utilize a container.
    """
    start = time.time()
    stdout = params['stdout'] if params else None
    container = params['container'] if params else None
    use_gpu = params['use_gpu'] if params else None
    sdir = params['sdir'] if params else None

    # When using a container, change all paths to be relative to its mounted directory (hideous, but works without changing other code)
    if container is not None:
        odir = split(sdir)[0]
        command = command.replace(odir, "/share")
        command = "singularity exec{} -B {}:/share {} {}".format(" --nv" if use_gpu else "", odir, container, command)
        write(stdout, command)

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

def clamp(value, lo, hi):
    return max(lo, min(value, hi))

def str2bool(string):
    if string is None:
        return string
    return string.lower() in ("yes", "true", "t", "1")

def get_time_date():
    """Returns date and time as Y-m-d H:M am/pm
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")

def get_time_string(seconds):
    """Returns seconds as Slurm-compatible time string
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "{:02d}:{:02d}:{:02d}".format(int(h), int(m), int(s))

def get_time_seconds(string):
    """Returns Slurm-compatible time string as seconds
    """
    while (len(string.split(":")) < 3):
        string = "00:" + string
    return sum(secs * int(digit) for secs,digit in zip([3600, 60, 1], string.split(":")))

def get_start(function_name):
    return "Starting {} at {}\n".format(basename(str(function_name)), get_time_date())

def get_finish(function_name):
    return "Finished {} at {}\n".format(basename(str(function_name)), get_time_date())

def print_start():
    print(get_start())
    return time.time()

def print_finish(start_time):
    print(get_finish())

def write(path, output):
    with open(path, 'a') as f:
        f.write(str(output) + "\n")

def record_start(params):
    """Record step start in timing log, and write to stdout.
    """
    step = params['step']
    timing_log = params['timing_log']
    stdout = params['stdout']
    with open(stdout, 'a') as f:
        f.write("\n=====================================\n")
        f.write(get_start(step))
        f.write("=====================================\n\n")
    write(timing_log, "{} start".format(time.time()))

def record_apptime(params, app_start_time, substep, *args):
    """Record substep duration in timing log.
    """
    timing_log = params['timing_log']
    apptime = time.time() - app_start_time
    line = "{} {}".format(apptime, substep)
    for arg in args:
        line += ' ' + str(arg)
    write(timing_log, line)

def record_finish(params):
    """Record cumulative step duration from timing log, and write to stdout.
    """
    sdir = params['sdir']
    step = params['step']
    timing_log = params['timing_log']
    stdout = params['stdout']
    cores_per_task = params['cores_per_task']
    use_gpu = params['use_gpu']
    global_timing_log = params['global_timing_log']
    sname = basename(sdir)
    task_start_time = 0
    task_total_time = 0
    max_apptimes = {}
    with open(timing_log, 'r') as f:
        for line in f.readlines():
            chunks = [x.strip() for x in line.strip().split(' ', 2) if x]
            if len(chunks) < 2:
                continue
            if chunks[1] == 'start':
                task_start_time = float(chunks[0])
                continue
            apptime = float(chunks[0])
            substep = int(chunks[1])
            task_total_time += apptime
            if substep not in max_apptimes:
                max_apptimes[substep] = apptime
            else:
                max_apptimes[substep] = max(apptime, max_apptimes[substep])
    ideal_walltime = get_time_string(sum(list(max_apptimes.values())))
    actual_walltime = get_time_string(time.time() - task_start_time)
    total_core_time = get_time_string(task_total_time * cores_per_task)
    with open(stdout, 'a') as f:
        f.write("\n=====================================\n")
        f.write(get_finish(step))
        f.write("Ideal walltime: {} (h:m:s)\n".format(ideal_walltime))
        f.write("Actual walltime: {} (h:m:s)\n".format(actual_walltime))
        f.write("Total core time: {} (h:m:s)\n".format(total_core_time))
        f.write("{} parallel cores per task\n".format(cores_per_task))
        f.write("Used GPU: {}\n".format(use_gpu))
        f.write("=====================================\n\n")
    write(global_timing_log, "{},{},{},{},{},{},{}".format(sname, step, ideal_walltime, actual_walltime, total_core_time, cores_per_task, use_gpu))
    run("chmod 770 {}".format(timing_log))
    run("chmod 770 {}".format(stdout))

def update_permissions(params):
    """Give user and group permissions to all generated files.
    """
    start_time = time.time()
    sdir = params['sdir']
    group = params['group']
    stdout = params['stdout']
    run("find {} -type f -print0 | xargs -0 -I _ chmod 770 _".format(sdir))
    run("find {} -type f -print0 | xargs -0 -I _ chgrp {} _".format(sdir, group))
    run("find {} -type d -print0 | xargs -0 -I _ chmod 2770 _".format(sdir))
    run("find {} -type d -print0 | xargs -0 -I _ chgrp {} _".format(sdir, group))
    write(stdout, "Updated file permissions, took {} (h:m:s)".format(get_time_string(time.time() - start_time)))

def generate_checksum(input_dir):
    """Return checksum of subject input files. This ensures re-computation if inputs change.
    """
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

def get_valid_filepath(template):
    """Return a valid path for new files. Used for log outputs.
    """
    path, ext = splitext(template)
    idx = 0
    valid_path = path + "_{:02d}".format(idx) + ext
    while exists(valid_path) and idx < 100:
        idx += 1
        valid_path = path + "_{:02d}".format(idx) + ext
    if idx >= 100:
        raise Exception("Could not find valid filepath for template {}".format(template))
    return valid_path, idx

def running_step(steps, *argv):
    """Return true if running any step in arguments.
    """
    for step in argv:
        if step in steps:
            return True
    return False

def generate_edge_list(vol_dir, path='lists/listEdgesEDIAll.txt'):
    """Not used during runtime. Generates a list of all possible edges from Freesurfer output.
    """
    with open(path,'w') as f:
        files = glob(join(vol_dir,"*_s2fa.nii.gz"))  # Assemble all files
        files = [abspath(vol) for vol in files]
        for a in files:
            for b in files:
                if a != b:
                    a1 = splitext(splitext(split(a)[1])[0])[0]
                    b1 = splitext(splitext(split(b)[1])[0])[0]
                    f.write("{},{}\n".format(basename(a1),basename(b1)))
