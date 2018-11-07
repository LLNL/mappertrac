#!/bin/bash
# Stops Parsl processes and deletes internal files (does not delete results)
killall ipcontroller &>/dev/null &
killall ipconfig &>/dev/null &
wait
rm -rf *.json
rm -rf *.core
rm -rf runinfo
rm -rf parsl_scripts
rm -rf cmd_parsl.auto.*
rm -rf slurm-*.out
rm -rf *.stdout
rm -rf .*.swp
