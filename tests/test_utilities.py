import pytest
from mappertrac.subscripts import *

def test_filesystem():
    smart_mkdir('/tmp/test_mappertrac')

    smart_remove('/tmp/test_mappertrac')