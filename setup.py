import os
from setuptools import find_packages
from distutils.core import setup
from glob import glob


install_requires = [
    'duckdb>=1.2.2',
    'pytz>=2024.1',
]


setup(
    name='duduckdb',
    version='0.1',
    description=('duduckdb prints a summary of disk or inodes usage, reading'
                 'information from a parquet file'),
    url='https://github.com/hpcleuven/duduckdb',
    package_dir={'': 'lib'},
    packages=find_packages(where='lib'),
    scripts=glob('bin/*'),
    python_requires='>=3.12',
    install_requires=install_requires,
)
