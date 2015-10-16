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
import time
from python_hiveish import logger

class PlatformArgs:
     def __init__(self, 
                  python_cmd,   #pointer to a python bin file where the site packages are installed, e.g., your venv bin 
                  temp_path,    #hdfs scratch directory
                  output_root,  #the root for results NO TRAILING SLASH
                  hdfs_prefix,  #the path prefix to be prepended to all input paths. e.g., hdfs://bigdata. NO TRAILING SLASH
                  script_root,  #path to PythonTools's exact location on HDFS server. NO TRAILING SLASH
                  switch,       #switch for hadoop platform; either "local", "frozen", or "hadoop"
                  cmdenvs,       #system variables to set before launching job  
                  num_mappers = None,  #can specify the number of mappers; if None, hadoop sets it                
                  num_reducers = None):  #can specify the number of reducers; if None, hadoop sets it      
        """A struct that represents the HDFS/MapReduce cluster's parameters. Not specific to a specific job, but for all jobs that run on this cluster"""  
        self.python_cmd = python_cmd
        self.temp_path = temp_path
        self.output_root = output_root 
        self.hdfs_prefix = hdfs_prefix
        self.script_root = script_root
        self.switch = switch 
        self.cmdenvs = cmdenvs 
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
 
        
def _hadoop_helper(platform_args, #instance of PlatformArgs
                   script_name,
                   in_name,      #input path    
                   out_name,     #output_path  
                   jobconfs,     #jobconfs 
                   output_as_text = False):
    """This is just a wrapper around hadoopy's launch method that allows one to swich platforms easily. 
       It also contains a few default args
       Not meant to be called directly except by function in this file that compose these arguments
       Please see the following for parameter definitions: http://hadoopy.readthedocs.org/en/latest/api.html
    """
    args = {}
    args["python_cmd"] = platform_args.python_cmd
    args['temp_path'] = platform_args.temp_path
    args['jobconfs'] =  jobconfs
    args['cmdenvs'] = platform_args.cmdenvs
    args["script_path"] = platform_args.script_root + "/" + script_name + ".py"
    args["in_name"] = in_name
    args["out_name"] = out_name
    args["make_executable"] = 1
    
    if output_as_text:
        args["use_seqoutput"] = False 

    if  platform_args.num_mappers:
        args["num_mappers"] = platform_args.num_mappers
            
    if  platform_args.num_reducers:
        args["num_reducers"] = platform_args.num_reducers

    logger.info("Launching {0} with {1} on {2} and writing to {3}".format(args["script_path"], str(jobconfs), args["in_name"], args["out_name"]))

    if platform_args.switch == "hadoop":
        launcher = hadoopy.launch
    elif platform_args.switch == "frozen":
        launcher = hadoopy.launch_frozen
    elif platform_args.switch == "local":
        launcher = hadoopy.launch_local 
    else:
        raise Exception("Unsupported Launch Switch: {0}".format(switch))
    launcher(**args)
    return out_name


def interlace_tables(platform_args,  #an instance of PlatformArgs    
                     input_paths):   #str or list of str; the input HDFS path(s) representing table 1. can contain asterisk paths
    """Simply takes all of the tables defined by input_paths, and writes them into the same table"""
    out_path = _hadoop_helper(platform_args, 
                              "identity_only_values",
                              input_paths, 
                              platform_args.output_root + "/{0}".format("interlace_tables") + "/%f" % time.time() , 
                              [],
                              output_as_text = True)    
    
    return out_path
    
    
def join(platform_args,               #an instance of PlatformArgs                        
        table_1_path,                 #str or list of str; the input HDFS path(s) representing table 1. THIS PATH OR LIST OF PATHS CANNOT CONTAIN ASTERISKS AND MUST BE FULLY COMPELTE!
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
    """When join_swtich == inner_join,
         Executes an SELECT T1 INNER JOIN T2 ON .. WHERE ..
         
       When join_switch == "left_join"
         Executes an SELECT T1 LEFT JOIN T2 ON .. WHERE ..
    """
    assert(join_switch == "inner_join" or join_switch == "left_join")
    
    full_input_list = []
    t1_path_str = ""
    if isinstance(table_1_path, list):
        t1_path_str = ",".join(["{0}{1}".format(platform_args.hdfs_prefix,i) for i in table_1_path])
        full_input_list += table_1_path
    else:
        t1_path_str = "{0}{1}".format(platform_args.hdfs_prefix, table_1_path)
        full_input_list += [table_1_path]

    t2_path_str = ""
    if isinstance(table_2_path, list):
        t2_path_str = ",".join(["{0}{1}".format(platform_args.hdfs_prefix,i) for i in table_2_path])
        full_input_list += table_2_path
    else:
        t2_path_str = "{0}{1}".format(platform_args.hdfs_prefix, table_2_path)
        full_input_list += [table_2_path]

    t1_filter_vals = []
    for i in table_1_filter_vals:
        t1_filter_vals.append(",".join(i))
    t1_filter_str = "|".join(t1_filter_vals)
        
    t2_filter_vals = []
    for i in table_2_filter_vals:
        t2_filter_vals.append(",".join(i))
    t2_filter_str = "|".join(t2_filter_vals)
        
    t1_key_str = ",".join([str(i) for i in table_1_key_columns])
    t2_key_str = ",".join([str(i) for i in table_2_key_columns])
    
    t1_target_col_str = ",".join([str(i) for i in table_1_target_columns])
    t2_target_col_str = ",".join([str(i) for i in table_2_target_columns])
    
    t1_filter_col_str = ",".join([str(i) for i in table_1_filter_columns])
    t2_filter_col_str = ",".join([str(i) for i in table_2_filter_columns])
    
    t1_invert_str = ",".join([str(i) for i in table_1_invert_flags]) 
    t2_invert_str = ",".join([str(i) for i in table_2_invert_flags]) 
    
    jobconfs =['table_1_path={0}'.format(t1_path_str),
              'table_1_key_columns={0}'.format(t1_key_str),  
              'table_1_filter_columns={0}'.format(t1_filter_col_str),
              'table_1_filter_vals={0}'.format(t1_filter_str), 
              'table_1_invert_filter_vals={0}'.format(t1_invert_str),
              'table_1_delimiter={0}'.format(table_1_delimiter),
              'table_2_path={0}'.format(t2_path_str),
              'table_2_delimiter={0}'.format(table_2_delimiter),
              'table_2_key_columns={0}'.format(t2_key_str),  
              'table_2_filter_columns={0}'.format(t2_filter_col_str),
              'table_2_filter_vals={0}'.format(t2_filter_str), 
              'table_2_invert_filter_vals={0}'.format(t2_invert_str),
              'table_1_target_columns={0}'.format(t1_target_col_str),  
              'table_2_target_columns={0}'.format(t2_target_col_str)
              ]

    out_path = _hadoop_helper(platform_args, 
                              join_switch,
                              full_input_list, 
                              platform_args.output_root + "/{0}".format(join_switch) + "/%f" % time.time() , 
                              jobconfs)
    return out_path
    
def _select_where_helper(platform_args,       #an instance of PlatformArgs  
                         in_path,             #str or list of str; the input HDFS path(s). Can contain asterisks. 
                         key_columns,         #list of ints; these are concatenated to form the key of returned rows (not used for select where, but used for select count * where groupby 
                         target_columns,      #list of ints; determines which columns to select
                         filter_columns,      #See below
                         filter_vals,         #list of lists of strings or ints; filter_columns[0] is checked to see if it is in filter_vals[0], same for the other columns
                         invert_flags,        #list of boolean; determines whether to select "NOT IN" instead of "IN" for each column (invert_flags[0] applied to filter_columns[0], etc)
                         delimiter,           #str; the delimiter the HDFS files at in_path are split by
                         switch):             #str; either "select_where" or ""select_count_star_where_and_groupby"
                      
    """internal helper function for the below two functions that simply switches between "select_where" and "select_count_star_where_and_groupby"""
    
    key_col_str = ",".join([str(i) for i in key_columns])
    
    target_col_str = ",".join([str(i) for i in target_columns])

    filter_col_str = "|".join([str(i) for i in filter_columns])
    
    ftr_vals = []
    for i in filter_vals:
        ftr_vals.append(",".join(i))
    filter_str = "|".join(ftr_vals)

    invert_str = "|".join([str(i) for i in invert_flags]) 
    
    jobconfs = ['target_columns={0}'.format(target_col_str),
               'filter_columns={0}'.format(filter_col_str),
               'filter_vals={0}'.format(filter_str), 
               'invert_filter_vals={0}'.format(invert_str),
               'key_columns={0}'.format(key_col_str),
               'delimiter={0}'.format(delimiter)]
    
    out_path = _hadoop_helper(platform_args, 
                              switch,
                              in_path, #nothing fancy needed here because hadoopy accepts single paths and lists
                              platform_args.output_root + "/{0}".format(switch) + "/%f" % time.time(), 
                              jobconfs)
    return out_path
 
    
def select_where(platform_args,        #see _select_where_helper
                 in_path = [],         #" "
                 key_columns = "*",    #" "
                 target_columns = "*", #" "
                 filter_columns = [],  #" "
                 filter_vals = [],     #" "
                 invert_flags = [],    #" "
                 delimiter = ","):     #" "
    """Executes a select where like statement. 
       Transforms easy to use list syntax into the jobconf syntax required by mappers.select_where
    
       This job uses identity_reducer as it's reducer and calls select_where.py

    """
    switch = "select_where"
    return _select_where_helper(**locals())

def  select_count_star_where_and_groupby(platform_args,        #see _select_where_helper
                                         in_path,              #" "
                                         key_columns,          #required here; acts as the groupby columns that will be used with select count(*) groupby
                                         target_columns = "*", #" "
                                         filter_columns = [],  #" "
                                         filter_vals = [],     #" "
                                         invert_flags = [],    #" "
                                         delimiter = ","):     #" "
    """Executes a select count(*) where .. groupby .. statement
       Transforms easy to use list syntax into the jobconf syntax required by mappers.select_where
    
       This job uses count_reducer as it's reducer and calls select_count_star_where_and_groupby.py
    """
    switch = "select_count_star_where_and_groupby"
    return _select_where_helper(**locals())


def select_where_interlace_multiple_tables(platform_args,            #see _select_where_helper
                                           mult_select_where_dict):  # a dictionary where the N keys are arbitrary names of the N tables, and the values of these keys
                                                                     # are keyword dictionaries that match the API (with one exception, see below) of select_where, 
                                                                     #e.g., mult_select_where_dict["table 1"]["in_path"] = ...
                                                                     #this dict can ALSO contain a special key called "hdfs_hotstart_path"; if it does, the select where is not executed
                                                                     #for that key and instead the existing hdfs path is used 
                                                                     
    """Executes a select where from N tables and then dumps the results from all N calls into a single output file. 
       All keys are suppressed so the output table looks just like the input table (modulo target columns and filtering criteria) 
    """ 
    out_paths = [] 
    for k, v in mult_select_where_dict.iteritems():
        out_paths.append(v["hdfs_hotstart_path"] if "hdfs_hotstart_path" in v else select_where(platform_args, **v)) #do all of the select wheres
    return interlace_tables(platform_args, out_paths) #concatenate the results   
        
    





    
    