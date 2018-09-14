import time
import os
import subprocess
import sys
from shutil import *
from os.path import exists, join, split, splitext, abspath
from os import system, environ
from subprocess import Popen, PIPE



def smart_copy(src,dest,force=False):
    if force or not exists(dest):
        copyfile(src,dest)

def run(command, env={}, ignore_errors=False, print_output=True, output_time=False, name_override="", working_dir=None, write_output=None):
    start = time.time()
    process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ, cwd=working_dir)
    line = ""
    while True:
        new_line = process.stdout.readline()
        new_line = str(new_line, 'utf-8')[:-1]
        if print_output:
            print(new_line)
        if write_output != None:
            with open(write_output, 'a') as f:
                f.write("{}\n".format(new_line))
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
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
    # environ.update(env)
    # return env

### Record environment in subprocess
# def get_env_vars(command):
#     variables = subprocess.Popen(
#     ["bash", "-c", "trap 'env' exit; source \"$1\" > /dev/null 2>&1",
#        "_", "yourscript"],
#     shell=False, stdout=subprocess.PIPE).communicate()[0]
#     print(variables)

def load_environ():
    machine_name = ''

    if environ['HOSTNAME'].startswith('quartz'):
        machine_name = 'quartz'
    elif environ['HOSTNAME'].startswith('surface'):
        machine_name = 'surface'
    elif environ['HOSTNAME'].startswith('ray'):
        machine_name = 'ray'
    else:
        print('Invalid cluster. Must run on quartz, surface, or ray.')
        exit(0)

    environ["FSLDIR"] = "/usr/workspace/wsb/tbidata/" + machine_name + "/fsl"
    environ["FSL_DIR"] = environ["FSLDIR"]
    environ["FSLOUTPUTTYPE"] = "NIFTI_GZ"
    environ["FSLMULTIFILEQUIT"] = "TRUE"
    environ["FSLTCLSH"] = environ["FSLDIR"] + "/bin/fsltclsh"
    environ["FSLWISH"] = environ["FSLDIR"] + "/bin/fslwish"
    environ["FSLGECUDAQ"] = "cuda.q"
    environ["FSL_BIN"] = environ["FSLDIR"] + "/bin"
    environ["FS_OVERRIDE"] = "0"
    environ["FSLTCLSH"] = environ["FSLDIR"] + "/bin/fsltclsh"
    environ["FSLWISH"] = environ["FSLDIR"] + "/bin/fslwish"

    environ["FREESURFER_HOME"] = "/usr/workspace/wsb/tbidata/surface/freesurfer"
    environ["LOCAL_DIR"] = environ["FREESURFER_HOME"] + "/local"
    environ["PERL5LIB"] = environ["FREESURFER_HOME"] + "/mni/share/perl5"
    environ["FSFAST_HOME"] = environ["FREESURFER_HOME"] + "/fsfast"
    environ["FMRI_ANALYSIS_DIR"] = environ["FSFAST_HOME"]
    environ["FSF_OUTPUT_FORMAT"] = "nii.gz"
    environ["MNI_DIR"] = environ["FREESURFER_HOME"] + "/mni"
    environ["MNI_DATAPATH"] = environ["FREESURFER_HOME"] + "/mni/data"
    environ["MNI_PERL5LIB"] = environ["PERL5LIB"]
    environ["MINC_BIN_DIR"] = environ["FREESURFER_HOME"] + "/mni/bin"
    environ["MINC_LIB_DIR"] = environ["FREESURFER_HOME"] + "/mni/lib"
    environ["SUBJECTS_DIR"] = environ["FREESURFER_HOME"] + "/subjects"
    environ["FUNCTIONALS_DIR"] = environ["FREESURFER_HOME"] + "/sessions"

    environ["LD_LIBRARY_PATH"] = "/usr/workspace/wsb/tbidata/" + machine_name + "/lib:" + \
                                 "/opt/cudatoolkit-7.5/lib64:" + \
                                 environ["LD_LIBRARY_PATH"]
    environ["PATH"] = environ["FREESURFER_HOME"] + "/bin:" + \
                      environ["MNI_DIR"] + "/bin:" + \
                      environ["FSLDIR"] + "/bin:" + \
                      environ["PATH"]
    environ["OS"] = "LINUX"
    if os.path.isfile("/usr/bin/display"):
        environ["FSLDISPLAY"] = "/usr/bin/display"
    if os.path.isfile("/usr/bin/convert"):
        environ["FSLCONVERT"] = "/usr/bin/convert"
    if machine_name in ['surface', 'ray']:
        environ["COMPILE_GPU"] = "1"

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

def printStart():
    print("Running {}".format(os.path.basename(sys.argv[0])))
    return time.time()

def printFinish(start_time):
    print("\n=====================================\n" +
          " {} took {} (h:m:s)".format(os.path.basename(sys.argv[0]), getTimeString(time.time() - start_time)) +
          "\n=====================================\n")

def getTimeString(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "{:d}:{:02d}:{:02d}".format(int(h), int(m), int(s))