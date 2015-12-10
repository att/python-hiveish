# python-hiveish
This library contains Python APIs to execute SQL like queries in Python, which are automatically translated into MapReduce jobs on your Hadoop cluster.   These APIs are high level wrappers around Hadoopy.  Specifically, it contains:
<ul>
<li>Pre-built MapReduce jobs with a SQL like interface. Internally, these are simply wrappers around MapReduces `jobconfs`
<li>"Plug n play" mappers and reducers that can be composed into new map reduce jobs. 
<li>Some basic functions to read and write HDFS files </li> 
</ul>

Bugs
=======
Please submit issues here on Github and I will try to respond to bug fixes. 

What is done
=======
<ul>
<li> Some basic reusable/composable Map and Reduce functions like: </li> 
<li> SELECT .. WHERE* .. #a select where like clause </li> 
<li> SELECT COUNT(*) WHERE .. GROUPBY .. #count the rows by frequency of the groupby clause
<li> SELECT .. FROM T1 INNER JOIN T2 ON ... WHERE .. #inner join </li> 
<li> SELECT .. FROM T1 LEFT JOIN T2 ON ... WHERE .. #left join </li> 
<li> SELECT .. WHERE .. FROM T1, SELECT .. WHERE .. FROM T2, ... INTO TABLE .. #Executes a select where from N tables and then dumps the results from all N calls into a single output file. 
*WHERE here is only equality currently, e.g., target columns are each in or not in target lists of values like:
     COL1 IN [VALS_1] and COL2 IN [VALS_2] and ...
</ul>

Known Bugs/Issues
=======
<ul>
<li>The SELECT WHERE functions accept a single string as path, or lists of strings as paths, and any of these paths can contain asterisks. HOWEVER, the JOIN functions, which also accept a single string as path, or lists of strings as paths, CANNOT handle paths containing asterisks*. This isn't a functionality-breaking deal, because the asterisk can be expanded prior to calling this function, and the list of paths can be arbitrarily long, but it is inconvienent. 
</li>
</ul>
*The technical reason for this is that for a JOIN, the reducer needs to know what "tables", i.e., the list of paths representing tables 1 and 2 respectively, each row comes from. In the mapper, the current file being processed is read from jobconfs as an exact path. This is then matched against the list of paths representing each table to determine what table the row is a part of. Matching this exact path (coming from jobconfs) to paths containing asterisks is hard and not currently implemented. 


ToDos
=======
<ul>
<li> Having platform_args in the join interface is kind of messy API wise. Fix this for API 2.0.</li>
<li> Documentation </li>
<li> Extend WHERE to inequalities </li>
<li> The rest of this TODO list </li> 
</ul>


USAGE APIS
=======
To simply USE, rather than EXTEND this library, the only file you should need to look at is mapreduce/execute.py

The following headers represent the APIs for the aformentioned four functions:
    
###INNER JOIN and LEFT JOIN
    def join(platform_args,           #an instance of PlatformArgs                        
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

	
###SELECT .. WHERE .. and  SELECT COUNT(*) WHERE .. GROUPBY .. 
	def select_where(platform_args,       #an instance of PlatformArgs  
                     in_path,             #str or list of str; the input HDFS path(s). Can contain asterisks. 
                     key_columns,         #list of ints; these are concatenated to form the key of returned rows (not used for select where, but used for select count * where groupby 
                     target_columns,      #list of ints; determines which columns to select
                     filter_columns,      #See below
                     filter_vals,         #list of lists of strings or ints; filter_columns[0] is checked to see if it is in filter_vals[0], same for the other columns
                     invert_flags,        #list of boolean; determines whether to select "NOT IN" instead of "IN" for each column (invert_flags[0] applied to filter_columns[0], etc)
                     delimiter,           #str; the delimiter the HDFS files at in_path are split by
                     switch):             #str; either "select_where" or ""select_count_star_where_and_groupby"

### SELECT .. WHERE .. FROM T1, SELECT .. WHERE .. FROM T2, ... INTO TABLE X
    def select_where_interlace_multiple_tables(platform_args,        #an instance of PlatformArgs  
                                           mult_select_where_dict):  # a dictionary where the N keys are arbitrary names of the N tables, and the values of these keys
                                                                     # are keyword dictionaries that match the API (with one exception, see below) of select_where, 
                                                                     #e.g., mult_select_where_dict["table 1"]["in_path"] = ...
                                                                     #exception: this dict can contain a special key called "hdfs_hotstart_path"; if it does, the select where is not executed
                                                                     #for that key and instead the existing hdfs path is used 
    
### INNER JOIN Example


    from python_hadoop_tools import hdfs_tools
    from python_hadoop_tools.testing import mapreduce
    from python_hadoop_tools.mapreduce import execute

    args = {}
    args["platform_args"] = .. execute.PlatformArgs(...) #populated with our HDFS specific parameters)
    
    args["table_1_path"] = "/projects/test/blah1"         
    args["table_2_path"]= "/projects/test/blah2"               
    args["table_1_filter_columns"]= [0] 
    args["table_2_filter_columns"]= [17|4]  
    args["table_1_filter_vals"]= ["A"]    
    args["table_2_filter_vals"]= [["B","C"]|["D"]]    
    args["table_1_invert_flags"]= [1]   
    args["table_2_invert_flags"]= [0|0]   
    args["table_1_key_columns"]= [0,1]  
    args["table_2_key_columns"]= [1,0] 
    args["table_1_target_columns"] = [3]
    args["table_2_target_columns"] = [2,5]
    args["table_1_delimiter"]= ","
    args["table_2_delimiter"]= "|" 
    
    args["join_switch"] = "inner_join"
        
    out_path = hotstart_path if hotstart_path else execute.join(**args)
    print("Reading from: {0}".format(out_path))
    hdfs_tools.print_hdfs(out_path)
    
Does a 
     
    SELECT T1.[COL3], T2.[COL2], T2.[COL5]
    FROM /projects/test/blah1 AS T1 INNER JOIN /projects/test/blah2 as T2
    ON T1.[COL0] = T2.[COL1] AND T1.[COL1]=T2.[COL0]
    WHERE T1.[COL0] NOT IN ["A"] AND T2.[COL17] IN ["B","C"] AND T2.[COL4] NOT IN ["D"]
        
	
pull requests
=======
Please issue a github PR for all changes to this library. 

versioning
=======
This project uses *strict adherence* to Semantic Versioning. Please keep the repository that way. See http://semver.org/. 
In short, if you:
1) break any api -> update major
2) add api functionality in a completely backwards compatible way -> update minor
3) fix something that does not change any api -> update patch