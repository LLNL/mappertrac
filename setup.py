import sys
from os.path import abspath, split, join
from setuptools import setup, find_packages

src_path = split(abspath(sys.argv[0]))[0]
src_path = join(src_path, 'mappertrac')

# ------------------------------------------------------------------------------
setup(
    name='mappertrac',
    description='Probabilistic tractography for high-performance computing',
    version='1.0.3',
    python_requires='>=3.7',
    author='Joseph Moon',
    author_email='moon15@llnl.gov',
    entry_points={
        'console_scripts': [
            'mappertrac = mappertrac.cli:main'
        ]
    },
    packages=find_packages(),
    include_package_data=True,
    package_data={'mappertrac' : ['data/*'] },
    install_requires=['pip>=21.2.4', 'parsl>=1.1.0', 'pytest>=6.2.4', 'scipy>=1.7.1', 'numpy>=1.21.2', 'gputil>=1.4.0'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)

# ------------------------------------------------------------------------------
