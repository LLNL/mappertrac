from sys import *
import numpy as np
import nibabel as nib

if len(argv) < 3:
    print "Usage: %s <input-file> <output>"
    exit(0)
    

data = nib.load(argv[1]).get_data().transpose()
data.tofile(argv[2] + "%d_%d_%d.raw" % (data.shape[0],data.shape[1],data.shape[2]))





 



