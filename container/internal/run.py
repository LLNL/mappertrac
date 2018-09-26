#!/usr/bin/env python3
import os
import subprocess
from sys import argv
from os.path import abspath,join,split,exists
from os import system
from shutil import copy
from subprocess import Popen, PIPE
from warnings import warn

if len(argv) < 2:

    print("""
    Usage: {0} <run-script>

    Example: {0} algorithm.py
    This will execute algorithm.py
    """.format(argv[0]))

    exit(0)

if not exists("/share/license.txt"):
    print("Freesurfer license (license.txt) not found in current directory")
    exit(0)
copy("/share/license.txt", os.environ["FREESURFER_HOME"])
system("cp /share/license.txt $FREESURFER_HOME/license.txt")
os.environ.pop('DEBUG', None) # DEBUG env triggers freesurfer to produce gigabytes of files

command = "./" + ' '.join(argv[1:])
process = Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, env=os.environ, cwd="/share")
while True:
    line = process.stdout.readline()
    line = str(line, 'utf-8')[:-1]
    print(line)
    if line == '' and process.poll() is not None:
        break
if process.returncode != 0:
    raise Exception("Non zero return code: %d" % process.returncode)
