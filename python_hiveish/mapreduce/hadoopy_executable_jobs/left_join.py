#!/usr/bin/env python

import hadoopy

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

if __name__ == "__main__":
    """
           When run on tables I_1, I_2 that share keys "1,2,3", 
           where I_1 has the shared keys in columns A,B,C 
           and I_2 has they shared keys in columns D,C,E,
           then run with join_left_reducer, this implements:
            SELECT I_1.COL_a, I_1.COL_b,.. I_2.COL_a, I_2.COL_b,
            FROM I_1 LEFT JOIN I_2 
            ON I_1.key1 = I_2.key1, I_1.key=I_2.key2,...
            WHERE I_1.filter_column_1  (not) in filter_vals_1_for_I_1, I_1.filter_column_2  (not) in filter_vals_2_for_I_1, ...
            and I_2.filter_column_1  (not) in filter_vals_1_for_I_2, I_2.filter_column_2  (not) in filter_vals_2_for_I_2, ...
            
            Implemented from: https://chamibuddhika.wordpress.com/2012/02/26/joins-with-map-reduce/ 
    """
    from python_hiveish.mapreduce.mappers import join_mapper as mapper
    from python_hiveish.mapreduce.reducers import join_left_reducer as reducer
    hadoopy.run(mapper, reducer, doc=__doc__)
