#!/usr/bin/env python3
import parsl
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from libsubmit.providers import SlurmProvider
from libsubmit.channels import SSHChannel
from libsubmit.launchers import SrunLauncher
# from parsl.configs.local_threads import config
# from parsl.data_provider.files import File

config = Config(
    executors=[
        IPyParallelExecutor(
            label='quartz_node',
            provider=SlurmProvider(
                'pbatch',
                channel=SSHChannel(
                    hostname='quartz.llnl.gov',
                    username=user_opts['cori']['username'],
                    script_dir=user_opts['cori']['script_dir']
                ),
                nodes_per_block=2,
                tasks_per_node=2,
                init_blocks=1,
                max_blocks=1,
                overrides=user_opts['cori']['overrides'],
                launcher=SrunLauncher,
            ),
            controller=Controller(public_ip=user_opts['public_ip']),
        )
    ],
    run_dir=get_rundir(),
)

parsl.load(config)