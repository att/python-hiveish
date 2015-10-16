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
import operator

def tb_topn_dict(path, limit=None, pprint = False):
    """Reads a path at HDFS that holds a dict, assumed to be a frequency dictionary), encoded as typed bytes. 
       Sorts the dict in descending order, then returns the top limit entries as a list of tuples
       
       Args:
           path (strng): HDFS path
           limit (None or int): the number of entries to return: if None, the entire sorted dict is returned
           pprint: print the top limit keys as they are added to the dict
        
        Returns: dict
    """
    top_dict = {}
    count = 0
    for key, value in sorted(dict(hadoopy.readtb(path)).items(), key=operator.itemgetter(1), reverse=True):
        count += 1
        top_dict[key] = [value]
        if pprint:
            print((key, value))
        if limit and count == limit:
            break
    return top_dict    
         
def read_hdfs_as_generator(path): 
    """Reads a path at HDFS and returns it line by line as a generator
       
       Args:
           path (strng): HDFS path
        
        Returns: strings (lines of the file) 
    """
    for i in hadoopy.readtb(path):
        yield i     
        
def count_hdfs_lines(path):
    """Simply counts the lines at the HDFS path
       
       Args:
           path (strng): HDFS path
        
        Returns: int (# lines in file) 
    """    
    count = 0
    for i in read_hdfs_as_generator(path):
        count += 1
    return count
    
def print_hdfs(path):
    """Reads a path at HDFS and prints it
       
       Args:
           path (strng): HDFS path
        
        Returns: None 
    """
    for i in read_hdfs_as_generator(path):
        print(i)
