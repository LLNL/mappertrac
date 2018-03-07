from sys import argv
from os.path import exists,join,split,splitext,abspath

if len(argv) < 2:
    print "Usage: %s <input-dir> <output-dir> <output.config>" % argv[0]
    exit(0)

if not exists(argv[1]):
    print "Input directory not found"
    exit(0)

output = ""

output += "Copied from FS example\n"

output += """
# FreeSurfer SUBJECTS_DIR
# T1 images and FreeSurfer segmentations are expected to be found here
# 
setenv SUBJECTS_DIR %s
""" % abspath(split(argv[1])[0])

output += """
# Output directory where trac-all results will be saved
# Default: Same as SUBJECTS_DIR
#
set dtroot = %s
""" % abspath(split(argv[2])[0])

output += """
# Subject IDs
#
set subjlist = (%s)
""" % split(argv[1])[1]

output += """
# Input diffusion DICOMs (file names relative to dcmroot)
# If original DICOMs don't exist, these can be in other image format
# but then the gradient table and b-value table must be specified (see below)
#
set dcmroot = %s
set dcmlist = (%s)
""" % (abspath(argv[1]),"hardi.nii.gz")

output += """
# Diffusion gradient tables (if there is a different one for each scan)
# Must be specified if they cannot be read from the DICOM headers
# The tables must have either three columns, where each row is a gradient vector
# or three rows, where each column is a gradient vector
# There must be as many gradient vectors as volumes in the diffusion data set
# Default: Read from DICOM header
#
set bveclist = (%s)

# Diffusion gradient table (if using the same one for all scans)
# Must be specified if it cannot be read from the DICOM headers
# The table must have either three columns, where each row is a gradient vector
# or three rows, where each column is a gradient vector
# There must be as many gradient vectors as volumes in the diffusion data set
# Default: Read from DICOM header
#
set bvecfile = %s
""" % (join(argv[1],"bvecs"),join(argv[1],"bvecs"))

output += """
# Diffusion b-value tables (if there is a different one for each scan)
# Must be specified if they cannot be read from the DICOM headers
# There must be as many b-values as volumes in the diffusion data set
# Default: Read from DICOM header
#
set bvallist = (%s)

# Diffusion b-value table (if using the same one for all scans)
# Must be specified if it cannot be read from the DICOM headers
# There must be as many b-values as volumes in the diffusion data set
# Default: Read from DICOM header
#
set bvalfile = %s
""" %  (join(argv[1],"bvals"),join(argv[1],"bvals"))

output += """
# Perform registration-based eddy-current compensation?
# Default: 1 (yes)
#
set doeddy = 1

# Rotate diffusion gradient vectors to match eddy-current compensation?
# Only used if doeddy = 1
# Default: 1 (yes)
#
set dorotbvecs = 1

# Fractional intensity threshold for BET mask extraction from low-b images
# This mask is used only if usemaskanat = 0
# Default: 0.3
#
set thrbet = 0.5


# MNI template
# Only used if doregmni = 1
# Default: $FSLDIR/data/standard/MNI152_T1_1mm_brain.nii.gz
#
set mnitemp = $FSLDIR/data/standard/MNI152_T1_1mm_brain.nii.gz


# Use brain mask extracted from T1 image instead of low-b diffusion image?
# Has no effect if there is no T1 data
# Default: 1 (yes)
#
set usemaskanat = 1

# Paths to reconstruct
# Default: All paths in the atlas
#
set pathlist = ( lh.cst_AS rh.cst_AS \
                 lh.unc_AS rh.unc_AS \
                 lh.ilf_AS rh.ilf_AS \
                 fmajor_PP fminor_PP \
                 lh.atr_PP rh.atr_PP \
                 lh.ccg_PP rh.ccg_PP \
                 lh.cab_PP rh.cab_PP \
                 lh.slfp_PP rh.slfp_PP \
                 lh.slft_PP rh.slft_PP )

# Number of path control points
# It can be a single number for all paths or a different number for each of the
# paths specified in pathlist
# Default: 7 for the forceps major, 6 for the corticospinal tract,
#          4 for the angular bundle, and 5 for all other paths
#
set ncpts = (6 6 5 5 5 5 7 5 5 5 5 5 4 4 5 5 5 5)

# List of training subjects
# This text file lists the locations of training subject directories
# Default: $FREESURFER_HOME/trctrain/trainlist.txt
#
set trainfile = $FREESURFER_HOME/trctrain/trainlist.txt

# Number of "sticks" (anisotropic diffusion compartments) in the bedpostx
# ball-and-stick model
# Default: 2
#
set nstick = 2

# Number of MCMC burn-in iterations
# (Path samples drawn initially by MCMC algorithm and discarded)
# Default: 200
#
set nburnin = 200

# Number of MCMC iterations
# (Path samples drawn by MCMC algorithm and used to estimate path distribution)
# Default: 7500
#
set nsample = 7500

# Frequency with which MCMC path samples are retained for path distribution
# Default: 5 (keep every 5th sample)
#
set nkeep = 5

# Reinitialize path reconstruction?
# This is an option of last resort, to be used only if one of the reconstructed
# pathway distributions looks like a single curve. This is a sign that the
# initial guess for the pathway was problematic, perhaps due to poor alignment
# between the individual and the atlas. Setting the reinit parameter to 1 and
# rerunning "trac-all -prior" and "trac-all -path", only for the specific
# subjects and pathways that had this problem, will attempt to reconstruct them
# with a different initial guess.
# Default: 0 (do not reinitialize)
#
set reinit = 0
"""


output_file = open(argv[-1],"w")
output_file.write(output)
output_file.close()










