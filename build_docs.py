#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

import subprocess
import os
import shutil


PACKAGE_NAMES = (
    'running',
    'tests',
)

ROOT = os.path.dirname(os.path.realpath(__file__))
os.chdir(ROOT)


# Generate API documentation
API = os.path.join(ROOT, 'api')

for package in PACKAGE_NAMES:
    rst_filename = os.path.join(ROOT, package + '.rst')
    if os.path.isfile(rst_filename):
        print('Removing {} file'.format(rst_filename))
        os.remove(rst_filename)
        
    (subprocess.Popen('sphinx-apidoc.exe {} -o api'.format(package), shell=True)).communicate()


# Build html documentation from all .rst sources.
BUILD = os.path.join(ROOT, '_build')

if os.path.isdir(BUILD):
    print('Removing previous version of _build')
    shutil.rmtree(BUILD)
elif os.path.isfile(BUILD):
    print('Removing _build file')
    os.remove(BUILD)

(subprocess.Popen('make html', shell=True)).communicate()


# Copy the generated html files to the docs folder for github pages usage.
SRC = os.path.join(BUILD, 'html')
DST = os.path.join(ROOT, 'docs')

if os.path.isdir(SRC):
    if os.path.isdir(DST):
        print('Removing previous version of docs')
        shutil.rmtree(DST)
    elif os.path.isfile(DST):
        print('Removing docs file')
        os.remove(DST)
    print('Copy the html files to the docs folder')
    shutil.copytree(SRC, DST)
