#!/usr/bin/env python3
from subscripts.utilities import run,is_integer,write,smart_copy
from os.path import join
from parsl.app.app import python_app
from shutil import copyfile

### These three functions parallelize FSL's "eddy_correct"

@python_app(executors=['debug'], cache=True)
def s_1_debug(params, inputs=[]):
    import time
    from subscripts.utilities import record_start,record_apptime,write
    record_start(params)
    start_time = time.time()
    sdir = params['sdir']
    container = params['container']
    if container:
        run("echo 'Testing Singularity on compute node\nShare dir is {}'".format(sdir), params)
    time.sleep(10)
    record_apptime(params, start_time, 1)

@python_app(executors=['debug'], cache=True)
def s_2_debug(params, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,record_finish,write
    start_time = time.time()
    time.sleep(15)
    record_apptime(params, start_time, 2)
    record_finish(params)

def setup_debug(params, inputs):
    sdir = params['sdir']
    container = params['container']
    if container:
        run("echo 'Testing Singularity on head node\nShare dir is {}'".format(sdir), params)
    s_1_future = s_1_debug(params, inputs)
    return s_2_debug(params, inputs=[s_1_future])
