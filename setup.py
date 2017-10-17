import os
import re
import setuptools
import sys

py_version = sys.version_info[:2]

_file_content = open(os.path.join(os.path.dirname(__file__), 'trio_amqp', 'version.py')).read()

rex = re.compile(r"""version__ = '(.*)'.*__packagename__ = '(.*)'""", re.MULTILINE | re.DOTALL)
VERSION, PACKAGE_NAME = rex.search(_file_content).groups()
description = 'AMQP implementation using asyncio'

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author="Matthias Urlichs",
    author_email='matthias@urlichs.de',
    url='https://github.com/python-trio/trio-amqp',
    description=description,
    long_description=open('README.rst').read(),
    download_url='https://pypi.python.org/pypi/trio_amqp',
    packages=[
        'trio_amqp',
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    platforms='all',
    license='BSD'
)
