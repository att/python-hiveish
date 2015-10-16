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
No try excepts here unless the MR job can complete without them!
Fail fast and have the exception stack show up in the Hadoop interface logs
"""

def identity_reducer(key, values):
    """Identity function; used when mapper did all the work"""
    for value in values:
        yield key, value

def identity_only_values_reducer(key, values):
    """Identity function; used when mapper did all the work
       This one kills the keys
       The purpose of this is to implement execute.interlace_tables, where the goal is to contatanate a bunch of tables
    """
    for value in values:
        yield value.strip(), ""
            
def count_reducer(key, values):
    """Counts the number of times key is present
    
        Args:
            key: string
            values: integer
        Yields:
            A tuple in the form of (key, value)
                key: same as input key
                value: int // sum(values)
    """
    yield key, sum([1 for x in values]) #len([x for x in values]) would also work, but not len(values) because it is not a list


def _join_records_splitter(values):
    """Purpose:
            Helper function used for JOIN reducers to split the records that came from table 1 and table 2. 
            
       Args:
            values: same set of values that was passed into the reducer
        
        Yields:
            two subsets of values that union to the exact set of values, the set of records from table 1, and the set of records from table 2
    """
    table_1_rows = []
    table_2_rows = []
    for v in values:
        if v["table"] == os.environ["table_1_path"]:
            table_1_rows.append(v)
        elif v["table"] == os.environ["table_2_path"]:
            table_2_rows.append(v)
        else:
            raise Exception("table matches neither table_1_path or table_2_path! Tsble: {0}".format(v["table"]))   
    return table_1_rows, table_2_rows
            
            
def join_inner_reducer(key, values):
    """ Purpose:
            To be used in conjunection with join_inner_mapper to implement INNER JOIN between two tables
        
        Notes: 
            RE the cache: values gets consumed when iterated over; it is a generator type input (type is something like 'Hadoopy-Group')
            you can't iterate over values twice like an Iterable (for i in values); the second time there's nothing there
            to iterate over them twice, therefore, we cache values
        
        Args:
             (TODO)

        Yields:
             key, value where value are the two rows joined by a comma regardless of their original delimiter. 
                         the original delimiter is used to split the respective tables. 
        
    """
    #for each key, which is handled prior to this function by the partiioner, 
    # there should only be one table 2 value. or else you have two rows in table 2 joining to table 1   
    table_1_rows, table_2_rows = _join_records_splitter(values)
    num_2_rows = len(table_2_rows) 
    for i in table_1_rows:
        if num_2_rows == 0:    
            pass #only difference from left join
        elif num_2_rows == 1:
           yield key, ",".join((i["row"].split(os.environ["table_1_delimiter"]) + table_2_rows[0]["row"].split(os.environ["table_2_delimiter"])))
        else:
            raise Exception("{0} table 2 rows have the same 'unique' join key!".format(num_2_rows))   



def join_left_reducer(key, values):
    """ Purpose:
             Exactly like the above (see that docstring) except it is a LEFT JOIN
         
        Yields:
             key, value where value is either:
                       1)  the two rows (one from table 1, one from table 2) delimited and joined by a comma regardless of their original delimiter. 
                           the original delimiter is used to split the respective tables. 
                       2) the oun-joined row from table 1  delimited by a comma, regardless of its original delimiter
    """
    #for each key, which is handled prior to this function by the partiioner, 
    # there should only be one table 2 value. or else you have two rows in table 2 joining to table 1   
    table_1_rows, table_2_rows = _join_records_splitter(values)
    num_2_rows = len(table_2_rows) 
    for i in table_1_rows:
        if num_2_rows == 0:    
            yield key, i["row"]
        elif num_2_rows == 1:
           yield key, i["row"] + "," + table_2_rows[0]["row"]
        else:
            raise Exception("{0} table 2 rows have the same 'unique' join key!".format(num_2_rows))   
        
        