import time,datetime,os,subprocess,sys,shutil,hashlib,grp,mmap,fnmatch,gzip,re,random
import distutils.dir_util
from glob import glob
from os.path import exists,join,split,splitext,abspath,basename,dirname,isdir,samefile,getsize
from shutil import copyfile,copytree,rmtree,ignore_patterns
from os import system,environ,makedirs,remove
from subprocess import Popen,PIPE
from itertools import islice

def smart_mkdir(path):
    if exists(path):
        return
    if not isdir(path):
        makedirs(path)

def smart_remove(path):
    """Remove all files and directories if they exist
    """
    if isdir(path):
        rmtree(path)
    elif exists(path):
        try:
            remove(path)
        except OSError:
            pass

def smart_copy(src, dest, exclude=[]):
    """Copy file or directory, while ignoring non-existent or equivalent files
    """
    if exists(dest) and samefile(src, dest):
        print("Warning: ignoring smart_copy because src and dest both point to {}".format(dest))
        return
    if not exists(dirname(dest)):
        smart_mkdir(dirname(dest))
    if isdir(src):
        tmp_root = join(dirname(dirname(__file__)), 'tmp')
        tmp = join(tmp_root, f'{basename(src)}_{random.randint(0,1000)}')
        smart_remove(tmp)
        copytree(src, tmp, ignore=ignore_patterns(*exclude))   # copy tree with excludes
        distutils.dir_util.copy_tree(tmp, dest)                # then copy tree without overwrite
        smart_remove(tmp)
    else:
        for pattern in exclude:
            if fnmatch.fnmatch(src, pattern):
                print('Did not copy {} because of exclude={}'.format(src, exclude))
                return
        copyfile(src, dest)

def run(command, params=None, ignore_errors=False, print_output=True, print_time=False, working_dir=None):
    """Run a command in a subprocess.
    Safer than raw execution. Can also write to logs and utilize a container.
    """
    start = int(time.time())
    work_dir = params['work_dir']
    stdout = params['stdout'] if (params and 'stdout' in params) else None
    container = params['container'] if (params and 'container' in params) else None
    use_gpu = params['use_gpu'] if (params and 'use_gpu' in params) else None
    container_cwd = params['container_cwd'] if (params and 'container_cwd' in params) else None

    # When using a container, change all paths to be relative to its mounted directory (hideous, but works without changing other code)
    if container is not None:
        command = command.replace(work_dir, "/mnt")
        command = (f'singularity exec {"--nv" if use_gpu else ""} ' +
            f'--cleanenv ' +
            f'--home /fake_home_dir ' +
            f'-B {work_dir}:/mnt {container} ' +
            f'sh -c "{command}"')
        print(command)
        if container_cwd:
            command = "cd {}; {}".format(container_cwd, command)
        if stdout:
            write(stdout, command)

    process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ, cwd=working_dir)
    line = ""
    while True:
        new_line = process.stdout.readline()
        new_line = str(new_line, 'utf-8')[:-1]
        if print_output and new_line:
            print(new_line)
        if stdout and not new_line.isspace():
            write(stdout, new_line)
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
        if stdout and not new_line.isspace():
            write(stdout, "Error: non zero return code")
            write(stdout, get_time_date())
        raise Exception("Non zero return code: {}\nCommand: {}".format(process.returncode, command))
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

def get_time_date():
    """Returns date and time as Y-m-d H:M am/pm
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")

def get_time_string(seconds):
    """Returns seconds as Slurm-compatible time string
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    time_string = "{:02d}:{:02d}:{:02d}".format(int(h), int(m), int(s))
    if h > 99999 or h < 0:
        return "00:00:00"
    return time_string

def write(path, output='', params={}):
    if params and 'container' in params and 'work_dir' in params:
        command = command.replace(params['work_dir'], "/mnt")
    # make path to file if not an empty string
    if dirname(path):
        smart_mkdir(dirname(path))
    with open(path, 'a') as f:
        f.write(str(output) + "\n")

def update_permissions(directory, params):
    """Give user and group permissions to all generated files.
    """
    start_time = time.time()
    stdout = params['stdout']
    run("find {} -type f -print0 | xargs -0 -I _ chmod 770 _".format(directory), params)
    run("find {} -type d -print0 | xargs -0 -I _ chmod 2770 _".format(directory), params)
    if 'group' in params:
        group = params['group']
        run("find {} -type f -print0 | xargs -0 -I _ chgrp {} _".format(directory, group), params)
        run("find {} -type d -print0 | xargs -0 -I _ chgrp {} _".format(directory, group), params)
    write(stdout, "Updated file permissions, took {} (h:m:s)".format(get_time_string(time.time() - start_time)))

def get_edges_from_file(file):
    edges = []
    with open(file) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            edges.append(edge.replace("_s2fa", "").strip().split(',', 1))
    return edges

def validate(file, params={}):
    file = file.strip()
    if not file.endswith('.nii.gz'):
        file = file + '.nii.gz'
    smart_mkdir('/var/tmp/')
    tmp = f'/var/tmp/tmp_{random.randint(0,1000)}_{basename(file)}'
    run(f"fslstats {file} -m | head -n 1 > {tmp}", params)
    time.sleep(5)
    with open(tmp, 'r') as f:
        mean = f.read().strip()
    smart_remove(tmp)

    assert is_float(mean), "Invalid mean value in {}".format(file)
    assert float(mean) != 0, "Zero mean value in {}".format(file)

def maskseeds(root_dir,input_dir,output_dir,low_threshold,high_threshold,high_threshold_thalamus,params):
    
    smart_remove(output_dir)
    smart_mkdir(output_dir)

    tmp_thalamus = join(root_dir, "tmp_thalamus.nii.gz")
    tmp = join(root_dir, "tmp.nii.gz")
        
    # Now create two transformed volumes with threshold 1 and 2
    run("fslmaths {} -thr {} -uthr {} -bin {}".format(join(root_dir,"FA.nii.gz"),low_threshold,high_threshold,tmp), params)
    run("fslmaths {} -thr {} -uthr {} -bin {}".format(join(root_dir,"FA.nii.gz"),low_threshold,high_threshold_thalamus,tmp_thalamus), params)
    
    for seed in glob(join(input_dir,"*s2fa.nii.gz")):

        region = split(seed)[1].split(".")[1].split("_")[0]
    
        if region == "thalamus":
            run("fslmaths {} -mas {} {}".format(seed, tmp_thalamus, join(output_dir,split(seed)[1])), params)
        else:
            run("fslmaths {} -mas {} {}".format(seed, tmp, join(output_dir,split(seed)[1])), params)
           
    smart_remove(tmp_thalamus)
    smart_remove(tmp)
    
def saveallvoxels(root_dir,cortical_dir,subcortical_dir,output_name,params):
    
    smart_remove(join(root_dir,"cort.nii.gz"))
    smart_remove(join(root_dir,"subcort.nii.gz"))
    
    all_vols = ""
    for vol in glob(join(cortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    
    run("find_the_biggest {} {}".format(all_vols,join(root_dir,"cort.nii.gz")), params)
        
    all_vols = ""
    for vol in glob(join(subcortical_dir,"*_s2fa.nii.gz")):
        all_vols += " " + vol
    
    run("find_the_biggest {} {}".format(all_vols,join(root_dir,"subcort.nii.gz")), params)
    run("fslmaths {} -add {} {} ".format(join(root_dir,"cort.nii.gz"),join(root_dir,"subcort.nii.gz"),output_name), params)
    run("fslmaths {} -bin {}".format(output_name,output_name), params)
