#!/usr/bin/env python3
from subscripts.utilities import run,is_integer,write,smart_copy
from os.path import join
from parsl.app.app import python_app
from shutil import copyfile

### These three functions parallelize FSL's "eddy_correct"

@python_app(executors=['head'], cache=True)
def s_1_debug(params, inputs=[]):
    sdir = params['sdir']
    container = params['container']
    if container:
        run("echo 'Testing Singularity on compute node\nShare dir is {}'".format(sdir), params)

def setup_debug(params, inputs):
    sdir = params['sdir']
    container = params['container']
    if container:
        run("echo 'Testing Singularity on head node\nShare dir is {}'".format(sdir), params)
    return s_1_debug(params, inputs=inputs)
