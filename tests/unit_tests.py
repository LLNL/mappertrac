import pytest
from mappertrac.subscripts import *
from os.path import *

script_dir = join(abspath(dirname(dirname(realpath(__file__)))), 'mappertrac')
test_dir = '/tmp/test_mappertrac'
params = {'container': abspath('image.sif'), 'work_dir':test_dir, 'stdout': join(test_dir, 'test.stdout')}

def test_filesystem():
    smart_mkdir(test_dir)

    test_txt = join(test_dir, 'test.txt')
    write(test_txt, get_time_date())
    write(test_txt, get_time_string(3600))
    update_permissions(test_dir, params)

    smart_remove(test_dir)

def test_fsl():
    smart_mkdir(test_dir)

    example_nifti = join(script_dir, 
        'data/example_inputs/sub-011591/anat/sub-011591_T1w.nii.gz')
    test_nifti = join(test_dir, 'test.nii.gz')
    smart_copy(example_nifti, test_nifti)
    run(f'fslinfo {test_nifti}', params)

    smart_remove(test_dir)

def test_edges():
    smart_mkdir(test_dir)

    edges = get_edges_from_file(join(script_dir, 
        'data/lists/list_edges_reduced.txt'))
    assert len(edges) == 930

    smart_remove(test_dir)

def test_validate():
    smart_mkdir(test_dir)

    example_nifti = join(script_dir, 
        'data/example_inputs/sub-011591/anat/sub-011591_T1w.nii.gz')
    test_nifti = join(test_dir, 'test.nii.gz')
    smart_copy(example_nifti, test_nifti)
    validate(test_nifti, params)

    smart_remove(test_dir)