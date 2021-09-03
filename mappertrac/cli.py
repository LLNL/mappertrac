#!/usr/bin/env python3
import argparse,parsl,os,sys,glob,shutil
from os.path import *
from mappertrac.subscripts import *

script_dir = abspath(os.path.dirname(os.path.realpath(__file__)))
cwd = abspath(os.getcwd())

def parse_args(args):

    parser = argparse.ArgumentParser()

    if not '--test' in sys.argv:
        parser.add_argument('inputs', nargs='+',
            help='Paths to BIDS subject folder(s).')

    parser.add_argument('--test', action='store_true',
        help='Test using the example subject.')

    workflow_group = parser.add_mutually_exclusive_group(required=True)

    workflow_group.add_argument('--preprocessing', '-s1', action='store_true',
        help='Run step 1: preprocessing.')

    workflow_group.add_argument('--bedpostx', '-s2', action='store_true',
        help='Run step 2: bedpostx.')

    workflow_group.add_argument('--freesurfer', '-s3', action='store_true',
        help='Run step 3: freesurfer.')

    workflow_group.add_argument('--probtrackx', '-s4', action='store_true',
        help='Run step 4: probtrackx.')

    parser.add_argument('--outputs', '-o', default='mappertrac_outputs/',
        help='Path to output directory.')

    parser.add_argument('--container', default=join(cwd, 'image.sif'),
        help='Path to Singularity container image.')

    scheduler_group = parser.add_mutually_exclusive_group()

    scheduler_group.add_argument('--slurm', action='store_true',
        help='Use the Slurm scheduler.')

    parser.add_argument('--nnodes', '-n', default=1,
        help='Scheduler: number of nodes.')

    parser.add_argument('--bank', '-b', default='asccasc',
        help='Scheduler: bank to charge for jobs.')

    parser.add_argument('--partition', '-p', default='pbatch',
        help='Scheduler: partition to assign jobs.')

    parser.add_argument('--walltime', '-t', default='11:59:00',
        help='Scheduler: walltime in format HH:MM:SS.')

    return parser.parse_args()

def main():

    args = parse_args(sys.argv[1:])

    if shutil.which('singularity') is None:
        raise Exception(f"Missing Singularity executable in PATH.\n\n" +
            f"Please ensure Singularity is installed: https://sylabs.io/guides/3.0/user-guide/installation.html")

    if not exists(args.container):
        raise Exception(f"Missing container image at {abspath(args.container)}\n\n" +
            f"Either specify another image with --container\n\n" +
            f"Or build the container with the recipe at: {join(script_dir, 'data/container/recipe.def')}\n\n" +
            f"Or download the container at: TODO needs upload\n")

    if args.test:
        args.inputs = join(script_dir, 'data/example_inputs/sub-*/')

    if isinstance(args.inputs, str):
        args.inputs = glob(args.inputs)

    all_params = {}

    # Copy reads to subject directory
    for input_dir in args.inputs:
        subject = basename(normpath(input_dir))

        if not subject:
            raise Exception(f'Invalid subject {subject} for input {input_dir}')

        subject_dir = abspath(join(args.outputs, subject))
        all_params[subject] = {
            'container': abspath(args.container),
            'work_dir': subject_dir,
            'stdout': join(subject_dir, 'worker.stdout'),
        }

    if args.slurm:
        executor = parsl.executors.HighThroughputExecutor(
            label="worker",
            address=parsl.addresses.address_by_hostname(),
            provider=parsl.providers.SlurmProvider(
                args.partition,
                launcher=parsl.launchers.SrunLauncher(),
                nodes_per_block=int(args.nnodes),
                init_blocks=1,
                max_blocks=1,
                worker_init=f"export PYTHONPATH=$PYTHONPATH:{os.getcwd()}",
                walltime=args.walltime,
                scheduler_options="#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.bank),
                move_files=False,
            ),
        )
    else:
        executor = parsl.executors.ThreadPoolExecutor(label="worker")

    config = parsl.config.Config(executors=[executor])
    parsl.set_stream_logger()
    parsl.load(config)

    # if args.ivar:
    #     results =  []
    #     for params in all_params.values():
    #         results.append(run_ivar(params))
    #     for r in results:
    #         r.result()

if __name__ == '__main__':
    main()
