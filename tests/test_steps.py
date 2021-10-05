import pytest,sys
from mappertrac.subscripts import *
from mappertrac.cli import main
from os.path import *

script_dir = join(abspath(dirname(dirname(realpath(__file__)))), 'mappertrac')
test_dir = '/var/tmp/test_mappertrac'
params = {
    'container': abspath('image.sif'), 
    'work_dir':test_dir,
    'output_dir': '/tmp',
    'stdout': join(test_dir, 'test.stdout'),
    'pbtx_sample_count': 1,
    'script_dir': script_dir,
    'input_dir': join(script_dir, 
        'data/example_inputs/sub-011591/anat/sub-011591_T1w.nii.gz')
}

def test_s1_freesurfer():
    smart_mkdir(test_dir)
    sys.argv = ['', '--test', '--s1_freesurfer']
    main()

def test_s2_bedpostx():
    sys.argv = ['', '--test', '--s2_bedpostx']
    main()

def test_s3_probtrackx():
    sys.argv = ['', '--test', '--s3_probtrackx']
    main()
    smart_remove(test_dir)
