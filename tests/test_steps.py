import pytest
from mappertrac.subscripts import *
from os.path import *

script_dir = join(abspath(dirname(dirname(realpath(__file__)))), 'mappertrac')
test_dir = '/tmp/test_mappertrac'
params = {'container': abspath('image.sif'), 'work_dir':test_dir, 'stdout': join(test_dir, 'test.stdout')}

def test_s1_freesurfer():
    smart_mkdir(test_dir)


def test_s2_bedpostx():
    pass


def test_s3_probtrackx():
    smart_remove(test_dir)
