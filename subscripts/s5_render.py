#!/usr/bin/env python3
from parsl.app.app import python_app

@python_app(executors=['s5'], cache=True)
def s5_1_start(params, inputs=[]):
    from subscripts.utilities import record_start
    record_start(params)

@python_app(executors=['s5'], cache=True)
def s5_2_render_target(params, input_file, output_file, inputs=[]):
    import time
    from subscripts.utilities import record_apptime
    start_time = time.time()
    record_apptime(params, start_time, 1)

@python_app(executors=['s5'], cache=True)
def s5_3_complete(params, inputs=[]):
    import time
    from subscripts.utilities import record_apptime,record_finish,update_permissions
    start_time = time.time()
    update_permissions(params)
    record_apptime(params, start_time, 2)
    record_finish(params)

def run_s5(params, inputs):
    render_list = params['render_list']
    s5_1_future = s5_1_start(params, inputs=inputs)
    s5_2_futures = []
    with open(render_list) as f:
        for render_target in f.readlines():
            input_file = ''
            output_file = ''
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