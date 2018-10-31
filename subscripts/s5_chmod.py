#!/usr/bin/env python3
from subscripts.config import executor_labels
from parsl.app.app import python_app

@python_app(executors=executor_labels)
def s5_chmod(sdir, group, stdout, checksum):
    from subscripts.utilities import run,write_start,write_finish
    write_start(stdout, "s5_chmod")
    run("chmod -R 770 {}".format(sdir), stdout)
    run("chgrp -R {} {}".format(group, sdir), stdout)
    write_finish(stdout, "s5_chmod")

def create_job(sdir, group, stdout, checksum):
    return s5_chmod(sdir, group, stdout, checksum)
