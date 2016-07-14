import logging
import os
import sys

try:
    from setuptools import setup
except ImportError:
    print("WARNING: setuptools not installed. Will try using distutils instead..")
    from distutils.core import setup

command = sys.argv[-1]
if command == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

long_description = open('README.rst').read()

install_requires = [
    'configargparse',
]

if sys.version_info < (3, 0):
    install_requires+= ['subprocess32']


setup(
    name='pypez',
    version="0.1.4",
    description='simple pipelines in python',
    long_description=long_description,
    author='Ben',
    author_email='ben.weisburd@gmail.com',
    url='https://github.com/bw2/pypez',
    py_modules=['pypez'],
    include_package_data=True,
    license="MIT",
    keywords='pype, unix, pipeline, pipelines, parallelization, SGE, LSF',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    test_suite='tests',
    install_requires=install_requires,
)
