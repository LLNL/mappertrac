#!/usr/bin/env bash
singularity exec --nv -B .:/share container.simg python3 /run.py $@
