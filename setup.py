"""
The MIT License (MIT)

Copyright (c) 2015 Tommy Carpenter

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import os
from setuptools import setup
from pip.req import parse_requirements

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

reqs = [str(ir.req) for ir in parse_requirements("requirements.txt", session=False)]

setup(
    name = "python-hiveish",
    version = "1.0.0",
    author = "Tommy Carpenter",
    author_email = "tommyjcarpenter@gmail.com, tommy@research.att.com",
    description = ("A hive-like interface wrapper around Hadoopy that allows SQL like queries ontop of MapReduce directly from Python"),
    license = "MIT",
    keywords = "python, hadoop, mapreduce",
    url = "https://github.com/tommyjcarpenter/python-hiveish",
    packages=[],
    long_description=read('README.md'),
    classifiers=[],
    install_requires=reqs
)
