#!/usr/bin/env python3
from parsl.app.app import python_app
from os.path import exists,join,basename
from subscripts.utilities import smart_mkdir,write
from shutil import copyfile

@python_app(executors=['s5'], cache=True)
def s5_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    record_start(params)

@python_app(executors=['s5'], cache=True)
def s5_2_render_target(params, input_file, output_file, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,run,write
    from os.path import splitext,join,exists
    start_time = time.time()
    sdir = params['sdir']
    run_vtk = join(sdir, 'run_vtk.py')
    stdout = params['stdout']
    if not exists(input_file):
        write(stdout, "Cannot find input file {}".format(input_file))
        return
    output_name = splitext(output_file)[0].strip()
    run('/opt/vtk/bin/vtkpython {} {} {}'.format(run_vtk, input_file, output_file), params)
    record_apptime(params, start_time, 1)

@python_app(executors=['s5'], cache=True)
def s5_3_complete(params, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions
    update_permissions(params)
    record_finish(params)

def run_s5(params, inputs):
    render_list = params['render_list']
    sdir = params['sdir']
    stdout = params['stdout']
    render_dir = join(sdir, 'render')
    smart_mkdir(render_dir)
    run_vtk = join(sdir, 'run_vtk.py')
    copyfile('subscripts/run_vtk.py', run_vtk)
    s5_1_future = s5_1_start(params, inputs=inputs)
    s5_2_futures = []
    with open(render_list) as f:
        for render_target in f.readlines():
            render_target = render_target.strip()
            render_name = basename(render_target).split('.')[0]
            input_file = join(sdir, render_target)
            output_file = join(render_dir, render_name + '.png')
            s5_2_futures.append(s5_2_render_target(params, input_file, output_file, inputs=[s5_1_future]))
    return s5_3_complete(params, inputs=s5_2_futures)