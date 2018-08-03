from sys import *
import numpy as np
import nibabel as nib
import pyevtk
from pyevtk.hl import gridToVTK
#from hl import gridToVTK

if len(argv) < 6:
    print "Usage: %s <cort.nii.gz> <subcort.nii.gz> <FA.nii.gz> <FAtwoways.nii.gz> <output>"
    exit(0)
    

cort    = nib.load(argv[1]).get_data().transpose()
subcort = nib.load(argv[2]).get_data().transpose()
FA      = nib.load(argv[3]).get_data().transpose()
EDI     = nib.load(argv[4]).get_data().transpose()
output = argv[5]

x = np.array(range(0,cort.shape[0]+1))*2
y = np.array(range(0,cort.shape[1]+1))
z = np.array(range(0,cort.shape[2]+1))


gridToVTK(output,x,y,z,cellData = {"FA" : FA, "Cort" : cort, "Subcort" : subcort, "EDI" : EDI})
 



