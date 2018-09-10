#!/usr/bin/env bash
singularity exec -B .:/share /usr/workspace/wsb/tbidata/Container/tools.simg python3 /run.py $@
