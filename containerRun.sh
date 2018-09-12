#!/usr/bin/env bash
singularity exec -B .:/share tools.simg python3 /run.py $@
