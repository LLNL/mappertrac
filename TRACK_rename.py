#!/usr/bin/env python3
import sys

old = 'batch1_chunk1'
new = 'batch1_chunk2'

files = ['TRACK.qsub', 'TRACK_s2a.qsub']

new_texts = {}
for file in files:
    with open(file) as f:
        new_texts[file] = f.read().replace('batch1_chunk1', 'batch1_chunk2')

for file in files:
    print('Renamed subject lists in {}'.format(file))
    with open(file, 'w') as f:
        f.write(new_texts[file])
