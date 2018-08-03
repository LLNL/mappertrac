from sys import *
import numpy as np
import nibabel as nib
from glob import glob
from os.path import *
from os import system

if len(argv) < 5:
    print "Usage: %s <volumes.nii.gz> <output-name>" % argv[0]
    exit(0)

result = nib.load(argv[1]).get_data().transpose()
dim = result.shape

result = 0 * result.flatten()

count = 1
for vol in argv[1:-1]:
    data = nib.load(vol).get_data().transpose().flatten()
    
    for i in np.where(data > 0)[0]:
       result[i] = count

    count += 1

print "Found %d labeled cells" % np.where(result > 0)[0].shape[0]
result.tofile("%s_%d_%d_%d.raw" % (argv[-1],dim[2],dim[1],dim[0]))
