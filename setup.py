#!/usr/bin/env python
from setuptools import setup, find_packages
__author__ = 'adamkoziol'

setup(
    name="autorun",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    license='MIT',
    author='Adam Koziol',
    author_email='adam.koziol@inspection.gc.ca',
    description='Automatic assembly pipeline',
    url='https://github.com/adamkoziol/autorun.git',
    long_description=open('README.md').read(),
    install_requires=['OLCTools'],
)
