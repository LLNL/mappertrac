#!/usr/bin/env python3
from subscripts.config import executor_labels
from parsl.app.app import python_app

@python_app(executors=executor_labels)
def s5_chmod(sdir, group, stdout, checksum):
    import time
    from subscripts.utilities import run,record_start,record_apptime,record_finish
    record_start(sdir, stdout, 's5')
    start_time = time.time()
    run("chmod -R 770 {}".format(sdir), stdout)
    run("chgrp -R {} {}".format(group, sdir), stdout)
    record_apptime(sdir, start_time, 's5')
    record_finish(sdir, stdout, cores_per_task, 's5')

def create_job(sdir, cores_per_task, stdout, checksum, args.group):
    return s5_chmod(sdir, group, stdout, checksum)
