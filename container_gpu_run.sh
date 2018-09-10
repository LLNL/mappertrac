#!/usr/bin/env bash
singularity exec --nv -B .:/share tools.simg python3 /run.py $@
