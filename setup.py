#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-exacttarget',
    version='1.7.1',
    description='Singer.io tap for extracting data from the ExactTarget API',
    author='Fishtown Analytics',
    url='http://fishtownanalytics.com',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_exacttarget'],
    install_requires=[
        'funcy==2.0',
        'singer-python==5.12.1',
        'python-dateutil==2.8.2',
        'voluptuous==0.14.1',
        'Salesforce-FuelSDK @ git+https://github.com/hotgluexyz/FuelSDK-Python.git#egg=Salesforce-FuelSDK', # USING THE HOTGLUE VERSION
    ],
    extras_require={
        'test': [
            'pylint==2.10.2',
            'astroid==2.7.3',
            'nose'
        ],
        'dev': [
            'ipdb==0.11'
        ]
    },
    entry_points='''
    [console_scripts]
    tap-salesforce-marketing=tap_exacttarget:main
    ''',
    packages=find_packages(),
    package_data={
        'tap_exacttarget': ['schemas/*.json']
    }
)
