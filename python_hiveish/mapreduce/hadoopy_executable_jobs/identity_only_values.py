#!/usr/bin/env python

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

import hadoopy

if __name__ == "__main__":
    """
         Identity mapper and reducer, but the reducer kills the keys. 
         Used to simply take multiple intput paths and write them into the same file. 
         Can be chained with other jobs, for example select wheres, to concatenate the results of multiple SELECT WHERE clauses
    """
    from python_hiveish.mapreduce.mappers import identity_mapper as mapper
    from python_hiveish.mapreduce.reducers import  identity_only_values_reducer as reducer
    hadoopy.run(mapper, reducer, doc=__doc__)
