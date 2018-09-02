#!/usr/bin/env python3

import setuptools

def read(fname):
    import os
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

long_desc = read('README.md')

version = '0.3'
release = '0.3.0'

setuptools.setup(
    name='litedb',
    version=release,
    url='https://github.com/LionelAuroux/litedb/',
    license='BSD-2clauses',
    author='Lionel Auroux',
    author_email='lionel.auroux@gmail.com',
    description="Access SQLite in an easier way",
    long_description= long_desc,
    keywords="sqlite db",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=[
        'litedb'
    ],
    test_loader='unittest:TestLoader',
    test_suite='tests'
)
