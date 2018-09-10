#!/usr/bin/env python3
from os import system,mkdir,remove,environ

machine_name = ''

if environ['HOSTNAME'].startswith('quartz'):
	machine_name = 'quartz'
elif environ['HOSTNAME'].startswith('surface'):
	machine_name = 'surface'
elif environ['HOSTNAME'].startswith('ray'):
	machine_name = 'ray'
else:
	print('Invalid cluster. Must run on quartz, surface, or ray.')
	exit(0)

environ["FSLDIR"] = "/usr/workspace/wsb/tbidata/" + machine_name + "/fsl"
environ["FSL_DIR"] = environ["FSLDIR"]
environ["FSLOUTPUTTYPE"] = "NIFTI_GZ"
environ["FSLMULTIFILEQUIT"] = "TRUE"
environ["FSLTCLSH"] = environ["FSLDIR"] + "/bin/fsltclsh"
environ["FSLWISH"] = environ["FSLDIR"] + "/bin/fslwish"
environ["FSLGECUDAQ"] = "cuda.q"
environ["FSL_BIN"] = environ["FSLDIR"] + "/bin"
environ["FS_OVERRIDE"] = "0"
environ["FSLTCLSH"] = environ["FSLDIR"] + "/bin/fsltclsh"
environ["FSLWISH"] = environ["FSLDIR"] + "/bin/fslwish"

environ["FREESURFER_HOME"] = "/usr/workspace/wsb/tbidata/surface/freesurfer"
environ["LOCAL_DIR"] = environ["FREESURFER_HOME"] + "/local"
environ["PERL5LIB"] = environ["FREESURFER_HOME"] + "/mni/share/perl5"
environ["FSFAST_HOME"] = environ["FREESURFER_HOME"] + "/fsfast"
environ["FMRI_ANALYSIS_DIR"] = environ["FSFAST_HOME"]
environ["FSF_OUTPUT_FORMAT"] = "nii.gz"
environ["MNI_DIR"] = environ["FREESURFER_HOME"] + "/mni"
environ["MNI_DATAPATH"] = environ["FREESURFER_HOME"] + "/mni/data"
environ["MNI_PERL5LIB"] = environ["PERL5LIB"]
environ["MINC_BIN_DIR"] = environ["FREESURFER_HOME"] + "/mni/bin"
environ["MINC_LIB_DIR"] = environ["FREESURFER_HOME"] + "/mni/lib"
environ["SUBJECTS_DIR"] = environ["FREESURFER_HOME"] + "/subjects"
environ["FUNCTIONALS_DIR"] = environ["FREESURFER_HOME"] + "/sessions"

environ["LD_LIBRARY_PATH"] = "/usr/workspace/wsb/tbidata/" + machine_name + "/lib:" + \
							 "/opt/cudatoolkit-7.5/lib64:" + \
							 environ["LD_LIBRARY_PATH"]
environ["PATH"] = environ["FREESURFER_HOME"] + "/bin:" + \
				  environ["FSLDIR"] + "/bin:" + \
				  environ["PATH"]
environ["OS"] = "LINUX"
if os.path.isfile("/usr/bin/display"):
	environ["FSLDISPLAY"] = "/usr/bin/display"
if os.path.isfile("/usr/bin/convert"):
	environ["FSLCONVERT"] = "/usr/bin/convert"
if machine_name in ['surface', 'ray']:
	environ["COMPILE_GPU"] = "1"