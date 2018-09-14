#!/usr/bin/env python3
import parsl
import os
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.threads import ThreadPoolExecutor
from libsubmit.providers import SlurmProvider, LocalProvider
from libsubmit.channels import SSHInteractiveLoginChannel, LocalChannel
from libsubmit.launchers import SrunLauncher
# from parsl.configs.local_threads import config
# from parsl.data_provider.files import File

# config = Config(
#     executors=[
#         IPyParallelExecutor(
#             label='quartz_multinode',
#             provider=SlurmProvider(
#                 'pdebug',
#                 channel=SSHInteractiveLoginChannel(
#                     hostname='quartz.llnl.gov',
#                     username='moon15',
#                     # script_dir=user_opts['cori']['script_dir']
#                 ),
#                 nodes_per_block=2,
#                 tasks_per_node=2,
#                 init_blocks=1,
#                 max_blocks=1,
#                 walltime="0:0:30",
#                 launcher=SrunLauncher,
#             )
#         )
#     ],
#     run_dir=get_rundir(),
# )

config = Config(
    executors=[
        # ThreadPoolExecutor()
        IPyParallelExecutor(
            label='test_multinode',
            # provider=LocalProvider()
            provider=SlurmProvider(
                'pdebug',
                label='quartz',
                channel=LocalChannel(),
                max_blocks=1,
                walltime="00:02:00",
                overrides="#SBATCH -o testParsl.stdout\n#SBATCH -A asccasc\n#SBATCH -p pdebug\n#SBATCH -t 2:00\n#SBATCH -N 1\n#SBATCH -J parslTest",
            )
        )
    ],
    # run_dir=get_rundir(),
)

# print("Length: {}".format(len(config)))

parsl.load(config)

@python_app
def write(x):
    from utilities import getTimeString
    with open('blah.txt', 'w') as f:
        f.write('Hello\n{}'.format(getTimeString(1848)))

write("argh").result()
