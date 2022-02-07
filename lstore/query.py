from platform import java_ver
from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from datetime import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        self.table.index.drop_index(self.table.key, primary_key) 
        pass


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        
        if len(columns) != self.table.num_columns:
            return False
        
        # rid counter
        rid = self.table.rid_counter
        self.table.rid_counter += 1

        # This inserts the an index for the record into the b+tree.
        self.table.index.create_index(self.table.key, columns[self.table.key], rid) 

        # metadata
        self.table.page.array[INDIRECTION_COLUMN].append(self.table.page.num_records)
        self.table.page.array[RID_COLUMN].append(rid)
        self.table.page.array[TIMESTAMP_COLUMN].append(datetime.now().timestamp())
        self.table.page.array[SCHEMA_ENCODING_COLUMN].append(schema_encoding)

        # user data
        for i in range(len(columns)):
            col_idx = i + 4

            self.table.page.array[col_idx].append(columns[i])

        # update page_directory and increment record counters
        self.table.page_directory[rid] = self.table.page.num_records + 1
        self.table.page.num_records += 1

        # if successful
        return True

        

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, index_value, index_column, query_columns):
        results = []

        # index.locate(column, value)
        rids = self.table.index.locate(index_column, index_value)

        for rid in rids:
            # make sure rid exists in the table
            if rid not in self.table.page_directory:
                continue

            offset = self.table.page_directory[rid]
            column_values = []

            # get values of each column if it is in the query_columns
            for i in range(self.table.num_columns):
                if query_columns[i - 4]:
                    column_values.append(self.table.page.array[i + 4][offset])

            record = Record(rid, self.table.key, column_values)
            results.append(record)

        return results


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # create new page, add new record to this page, link new record to original record in base page
        pass


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        #use this function for getting a list of RIDS within the begin and end range: locate_range(self, begin, end, column)
        rids = self.table.index.locate_range(start_range, end_range, aggregate_column_index)

        if len(rids) == 0:
            return False

        total_sum = 0
        for rid in rids:
            offset = self.table.page_directory[rid]
            total_sum += self.table.page.array[aggregate_column_index][offset]

        return total_sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
