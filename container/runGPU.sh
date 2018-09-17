#!/usr/bin/env bash
singularity exec --nv -B .:/share container/image.simg python3 /run.py $@
