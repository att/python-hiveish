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

"""
Warning; here be dragons. Documentation needed. 

No try excepts here unless the MR job can complete without them!
Fail fast and have the exception stack show up in the Hadoop interface logs
"""

def _filtering_parsing_helper(filter_cols_key, filter_vals_key, filter_invert_key):
    filter_vals = os.environ[filter_vals_key].split("|")
    inverts = [int(y) for y in os.environ[filter_invert_key].split("|")]
    filter_dict = {}
    for xindex, x in enumerate([int(y) for y in os.environ[filter_cols_key].split("|")]):
        filter_dict[x] = {} 
        filter_dict[x]["filter_vals"] = filter_vals[xindex].split(",")
        filter_dict[x]["invert"] = inverts[xindex]
    return filter_dict
            
def _filtering_passed_helper(filter_dict, vals):
    yield_row = True
    for filter_col in filter_dict.keys():
        if (filter_dict[filter_col]["invert"] and vals[filter_col] in filter_dict[filter_col]["filter_vals"]) or (not filter_dict[filter_col]["invert"] and vals[filter_col] not in filter_dict[filter_col]["filter_vals"]):
            yield_row = False
            break     
    return yield_row
    
def _kv_helper(cache, value):
    """shared code between select_where and select_join
    
       splits vals, see if filtering passes, forms the key from key_columns and forms the values from target_columns
    """
    vals = [v.replace('"','') for v in value.split(cache["delimiter"])]
    if "filtering" not in cache or _filtering_passed_helper(cache["filtering"], vals):  #yield if filtering criteria met or no filtering criteria    
        k = "+".join(vals) if cache["key_columns"] == "*" else "+".join(vals[l] for l in cache["key_columns"])            
        v = ",".join(vals) if cache["target_columns"] == "*" else ",".join([vals[l] for l in cache["target_columns"]])
        return k, v
    return None, None
        
def identity_mapper(key, value):
    """ Does Nothing; used when all work done in reduce phase  
    """
    yield key, value
        
def token_count_mapper(key, value):
    """ Purpose:
          Splits lines of each file by whitespace (value holds lines), and emits (token, 1) for each token in the line.
          When combined with a basic count reducer, implements wordcount.
        
        Args:
            key: byte offset (not used in this function)
            value: (string)
        Yields:
            A series of tuples of the form (key, 1)   
    """
    for token in value.split():
        yield token, 1

def select_where(key, value, cache={}):
    """
        PURPOSE: 
           When combined with an identiy reducer this implements:
            
            SELECT (k, v)
                where k = target_column_1+target_column_2+...,+target_column_N,
                where v = target_column_1, ..., target_column_N
            FROM (input dataset)
            GROUP BY target_column_1, ..., target_column_N;
            WHERE filter_column_1 (not) in [filter_vals_1] and filter_column_2 (not) in [filter_vals_2] and ...
          
          When combined with a count reducer this implements:
             
            SELECT (k, v)
                where k = target_column_1+target_column_2+...,+target_column_N,
                where v = count(*)
            FROM (input dataset)
            GROUP BY target_column_1, ..., target_column_N;
            WHERE filter_column_1 (not) in [filter_vals_1] and filter_column_2 (not) in [filter_vals_2] and ...
      
        Args:
            key: byte offset (not used in this function; returned as is)
            value: (string)
            via jobconfs (MANDATORY) - target_columns: can be 
                                                           1) "*" : all columns selected as return value
                                                           2) comma delimited list of ints as a string like "1,2,3" 

            via jobconfs (MANDATORY) - key_columns: can be 
                                                           1) "*" : all columns selected as return value
                                                           2) comma delimited list of ints as a string like "1,2,3"
                                                           
            via jobconfs (OPTIONAL) - delimiter: the delimter the file is split on
            via jobconfs (OPTIONAL) - filter_columns : pipe delimited list of ints like: 1|2|3. This list will be split
                                                       and entry i will be used with filter vals list i
            via jobconfs (OPTIONAL) - filter_vals: is a pipe delimited list of comma delimited list of strings as a string like a,b,c|d,e,f|,..
                                                    this list is split on | and entry i is used as the filter list for filter column i
            via jobconfs (OPTIONAL) - invert_filter_vals: pipe delimited list of boolean integers, e.g., 0|1|0...
                                                          this list is split, and used to trigger "not in" instead of in (like WHERE NOT)try i is 1, then values are selected where filter column i is 
                                                          NOT in filter_vals list i
                                                          
            All three of these must be passed in or nothing happens (no where clause) 
            
             EXAMPLE:
                     jobconf = ['filter_columns=1|2, filtervals=a,b|c,d, invert_filter_vals = 0|1
                     
                     Does a 
                            SELECT * where column[1] in ["a","b"] and column[2] NOT in ["c,d"]
            
            Note that you can pass in the same column twice where invert_filter_vals = 0 then 1, e.g.,:
               jobconf = ['filter_columns=1|1, filtervals=a,b|c,d, invert_filter_vals = 0|1
            to get a "column[1] in ["a","b"] but not in ["c","d"] effect. 
            
        Yields:
            (k, v)
                where k = target_column_1+target_column_2+...,+target_column_N,
                where v = target_column_1, ..., target_column_N)
            for the subset of (key, value) inputs matching the where clause
    """
    if not "filtering" in cache and "filter_columns" in os.environ and "filter_vals" in os.environ and "invert_filter_vals" in os.environ:
        cache["filtering"] = _filtering_parsing_helper("filter_columns", "filter_vals", "invert_filter_vals")
        
    if not "delimiter" in cache:
        cache["delimiter"] = os.environ["delimiter"]

    if not "target_columns" in cache:
        if os.environ["target_columns"] == "*":
            cache["target_columns"] = "*"  
        else:
            cache["target_columns"] = [int(x) for x in os.environ["target_columns"].split(",")] #list

    if not "key_columns" in cache:
        if os.environ["key_columns"] == "*":
            cache["key_columns"] = "*"  
        else:
            cache["key_columns"] = [int(x) for x in os.environ["key_columns"].split(",")] #list
                  
    k, v = _kv_helper(cache, value)
    if k and v:
        yield k,v                 
                       

def join_mapper(key, value, cache={}):
    """"table" refers to all files in one HDFS root directory below:
                 
        PURPOSE: 
           Very similar to "select_where_mapper" except the key output is different. 
           Outputs (key_1+key_2,..., value) where key_1, key_2,... are given by the columns specified in key_columns. 
            
           When run on tables I_1, I_2 that share keys "1,2,3", 
           where I_1 has the shared keys in columns A,B,C 
           and I_2 has they shared keys in columns D,C,E,
           then run with join_inner_reducer, this implements:
            SELECT I_1.COL_a, I_1.COL_b,.. I_2.COL_a, I_2.COL_b,
            FROM I_1 INNER JOIN I_2 
            ON I_1.key1 = I_2.key1, I_1.key=I_2.key2,...
            WHERE I_1.filter_column_1  (not) in filter_vals_1_for_I_1, I_1.filter_column_2  (not) in filter_vals_2_for_I_1, ...
            and I_2.filter_column_1  (not) in filter_vals_1_for_I_2, I_2.filter_column_2  (not) in filter_vals_2_for_I_2, ...
      
        Args:
            key: byte offset (not used in this function; returned as is)
            value: (string)
            via jobconfs (MANDATORY)  - table_1_path='...' string representing the HDFS path(s) (if multiple, should be a string with commas between the individual paths) of the files of "table 1". used to parse out the key columns from this table when table 2 has the same keys but in different columns
            via jobconfs (MANDATORY)  - table_2_path='...' " "
            via jobconfs (MANDATORY)  - table_1_key_columns=1,2,3': comma delimited list of ints as a string like "1,2,3"
            via jobconfs (MANDATORY)  - table_2_key_columns=1,2,3': " "
            via jobconfs (MANDATORY)  - table_1_delimiter: the delimter the file 1 is split on
            via jobconfs (MANDATORY)  - table_2_delimiter: " "
            via jobconfs (MANDATORY)  - table_1_target_columns: can be 
                                                           1) "*" : all columns selected as return value
                                                           2) comma delimited list of ints as a string like "1,2,3" 
            via jobconfs (MANDATORY)  - table_2_target_columns: " " 
                                                                       
            For example and usage of the following 6 parameters, see the docstring of select_where;
               they are the same except duplicated for "table_1" and table_2"
            
            via jobconfs (OPTIONAL) - table_1_filter_columns
            via jobconfs (OPTIONAL) - table_1_filter_vals
            via jobconfs (OPTIONAL) - table_1_invert_filter_vals
            via jobconfs (OPTIONAL) - table_2_filter_columns
            via jobconfs (OPTIONAL) - table_2_filter_vals
            via jobconfs (OPTIONAL) - table_2_invert_filter_vals
        Yields:
            a subset of the (key_1+key_2+..., value) for each input pair
    """
    PREFIX = None
    INPUT = os.environ["mapreduce_map_input_file"]
    
    #Determine what table this row is a part of. 
    #To resolve the known issue listed in the readme about paths not containing asterisks, this needs 
    #to be updated to include some fancy regex logic
    for t1p in os.environ["table_1_path"].split(","):
        if INPUT.startswith(t1p):
            PREFIX = "1"
            break
    if not PREFIX:
        for t2p in os.environ["table_2_path"].split(","):
            if INPUT.startswith(t2p):
                PREFIX = "2"
                break
    if not PREFIX:
        raise Exception("Bug: File {0} matches neither input path 1 ({1}) or input path 2 ({2})".format(INPUT, os.environ["table_1_path"], os.environ["table_2_path"]))
    
    TABLE = os.environ["table_{0}_path".format(PREFIX)]
    
    if not "filtering" in cache and "table_{0}_filter_columns".format(PREFIX) in os.environ and "table_{0}_filter_vals".format(PREFIX) in os.environ and "table_{0}_invert_filter_vals".format(PREFIX) in os.environ:
        cache["filtering"] = _filtering_parsing_helper("table_{0}_filter_columns".format(PREFIX), "table_{0}_filter_vals".format(PREFIX), "table_{0}_invert_filter_vals".format(PREFIX))    
        
    if not "key_columns" in cache:
        cache["key_columns"] = [int(x) for x in os.environ["table_{0}_key_columns".format(PREFIX)].split(",")] #list

    if not "target_columns" in cache:
        if os.environ["table_{0}_target_columns".format(PREFIX)] == "*":
            cache["target_columns"] = "*"  
        else:
            cache["target_columns"] = [int(x) for x in os.environ["table_{0}_target_columns".format(PREFIX)].split(",")] #list
            
    if not "delimiter" in cache:
        cache["delimiter"] = os.environ["table_{0}_delimiter".format(PREFIX)]

    k, v = _kv_helper(cache, value)
    if k and v:
        outdict = {}
        outdict["table"] = TABLE    
        outdict["row"] = v
        yield k, outdict
