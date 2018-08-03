from sys import *
import numpy as np
import nibabel as nib
import pyevtk
from pyevtk.hl import gridToVTK
from os.path import split,splitext
#from hl import gridToVTK

if len(argv) < 3:
    print "Usage: %s <file1.nii.gz> ... <fileN.nii.gz> <output>"
    exit(0)
    
fields = dict()

data = nib.load(argv[1]).get_data().transpose()

x = np.linspace(0, data.shape[0], data.shape[0])*2
y = np.linspace(0, data.shape[1], data.shape[1])
z = np.linspace(0, data.shape[2], data.shape[2])

for f in argv[1:-1]:
    print f
    fields[splitext(split(f)[1])[0]] = nib.load(f).get_data().transpose()
    




gridToVTK(argv[-1],x,y,z,pointData = fields)
 



