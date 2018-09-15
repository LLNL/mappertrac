#!/usr/bin/env bash
singularity exec -B .:/share container/image.simg python3 /run.py $@
