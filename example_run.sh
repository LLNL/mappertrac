#!/bin/bash
BASE=example.qsub
WITH_PASSWORD=example_with_password.qsub
echo "Enter password:"
read -s PASSWORD
echo "$(cat $BASE) --password=$PASSWORD" > $WITH_PASSWORD
msub $WITH_PASSWORD
rm $WITH_PASSWORD