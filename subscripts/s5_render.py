#!/usr/bin/env python3
from parsl.app.app import python_app
from os.path import exists,join,basename
from subscripts.utilities import smart_mkdir
from shutil import copyfile

@python_app(executors=['s5'], cache=True)
def s5_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    record_start(params)

@python_app(executors=['s5'], cache=True)
def s5_2_render_target(params, input_file, output_file, inputs=[]):
    # import time, nibabel, pyevtk, numpy
    import time
    from subscripts.utilities import record_apptime,run,write
    from os.path import splitext,join
    start_time = time.time()
    sdir = params['sdir']
    format_vtk = join(sdir, 'format_vtk.py2')
    run_vtk = join(sdir, 'run_vtk.py2')
    output_name = splitext(output_file)[0].strip()
    vtr_file = output_name + '.vtr'
    run('python2.7 {} {} {}'.format(format_vtk, input_file, output_name), params)
    run('python2.7 {} {} {}'.format(run_vtk, vtr_file, output_file), params)

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
    render_dir = join(sdir, 'render')
    smart_mkdir(render_dir)
    format_vtk = join(sdir, 'format_vtk.py2')
    run_vtk = join(sdir, 'run_vtk.py2')
    copyfile('subscripts/format_vtk.py2', format_vtk)
    copyfile('subscripts/run_vtk.py2', run_vtk)
    s5_1_future = s5_1_start(params, inputs=inputs)
    s5_2_futures = []
    with open(render_list) as f:
        for render_target in f.readlines():
            render_target = render_target.strip()
            render_name = basename(render_target).split('.')[0]
            input_file = join(sdir, render_target)
            output_file = join(render_dir, render_name + '.png')
            print(input_file)
            print(output_file)
            s5_2_futures.append(s5_2_render_target(params, input_file, output_file, inputs=[s5_1_future]))
    return s5_3_complete(params, inputs=s5_2_futures)

# if args.render:
#     import nibabel as nib
#     import pyevtk
#     from pyevtk.hl import gridToVTK
#     for subject in subjects:
#         sname = subject['sname']
#         sdir = join(odir, sname)
#         log_dir = join(sdir,'log')
#         render_dir = join(sdir, 'render')
#         smart_mkdir(render_dir)
#         stdout, idx = get_valid_filepath(join(log_dir, "render.stdout"))
#         params = {
#             'sdir': sdir,
#             'container': container,
#             'stdout': stdout,
#         }
#         for target in open(args.render_targets, 'r').readlines():
#             # Ugly, but only way I could get OSMesa working
#             run("python2.7 subscripts/render.py2 {} {}".format('a', 'b'), params)
#         update_permissions_base(render_dir, args.group)