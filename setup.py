#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Install the "runningtrack" package'''


from __future__ import print_function, division

from setuptools import setup, find_packages


VERSION = open('VERSION').read().splitlines()[0].strip()
README = open('README.rst').read().strip()

setup(name='runningtrack',
      version=VERSION,
      author='Paulo Lopes',
      author_email='palopes@cisco.com',
      url='https://paulopes.github.io/running/',
      description='Runs functions concurrently in the background, scheduled and with timeout/retries if they get stuck.',
      long_description=README,
      packages=find_packages('runningtrack'),
      install_requires=[
          # Only requires some python standard packages.
          # It can use pyyaml and colorama if installed,
          # but it does not require them.
      ],
      test_suite='tests',
     )
