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

from python_hiveish import hdfs_tools, logger
from python_hiveish.mapreduce import execute

"""Same exact API as execute.join; luckily select_where is a subset of this"""
def   join_test(platform_args,               #an instance of PlatformArgs                        
                table_1_path,                 #str; the input HDFS path
                table_2_path,                 #" "   
                table_1_key_columns,          #list of ints; represents the columns that form the composite join path
                table_2_key_columns,          #" "
                table_1_delimiter =",",       #str; the delimiter the HDFS files at table_1_path are split by
                table_2_delimiter=",",        #" "
                table_1_filter_columns = [],  #See below
                table_2_filter_columns = [],  #" "
                table_1_filter_vals= [],      #list of lists of strings or ints; table_1_filter_columns[0] is checked to see if it is in table_1_filter_vals[0], same for the other columns
                table_2_filter_vals= [],      #" "
                table_1_invert_flags= [],     #list of boolean; determines whether to select "NOT IN" instead of "IN" for each column (invert_flags[0] applied to filter_columns[0], etc)
                table_2_invert_flags= [],      #" " 
                table_1_target_columns = "*", #the columns from table 1 to select
                table_2_target_columns = "*", #the columns from table 2 to select
                join_switch = "inner_join"):  # either "inner_join" or "left_join" 
    """This is for testing.
       Does an inner join and counts the number of rows in the JOIN. 
       Selects from table 1 that match the where clause, and counts the number of rows. 
       Selects from table 2 that match the where clause, and counts the number of rows. 
       This helps debug cases where the number of rows in the inner join appears to be wrong. 
       
       TODO: This does not tell you all of the information needed, yet, to know if a a LEFT or INNER join worked
             For an INNER JOIN, you need to know whether there are any rows in table 2 matching the keys from table 1, how many, etc
             THis only tells you how many rows from table 1 and 2 met the respective where clauses
             
             FOr a LEFT JOIN you need to verify that the number of rows in table 1 == number of rows in the join,]
             however this doesn't tell you whether the rows from t2 were actually joined to the rows in t1
             
             
    """
    join_out_path = execute.join(**locals()) 
    logger.info("lines in JOIN: {0}".format(hdfs_tools.count_hdfs_lines(join_out_path)))
    t1_test =   execute.select_where(platform_args = platform_args,
                                     in_path = table_1_path,          
                                     key_columns = table_1_key_columns,                     
                                     filter_columns = table_1_filter_columns,        
                                     filter_vals = table_1_filter_vals,            
                                     invert_flags = table_1_invert_flags,          
                                     delimiter = table_1_delimiter)
    logger.info("matching lines in table 1: {0}".format(hdfs_tools.count_hdfs_lines(t1_test)))
    t2_test =   execute.select_where(platform_args = platform_args,
                                     in_path = table_2_path, 
                                     key_columns = table_2_key_columns,                     
                                     filter_columns = table_2_filter_columns,        
                                     filter_vals = table_2_filter_vals,            
                                     invert_flags = table_2_invert_flags,          
                                     delimiter = table_2_delimiter)
    logger.info("matching lines in table 2: {0}".format(hdfs_tools.count_hdfs_lines(t2_test)))  
            