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
        self.table: Table = table
        self.num_updates = 0
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        #Deleting Index of primary key
        #self.table.index.drop_index(self.table.key, primary_key) 
        rids = self.table.index.drop_index(self.table.key, primary_key)

        for rid in rids:
            # if record doesnt exist or has already been deleted
            if rid == -1 or rid not in self.table.page_directory:
                return False
            
            (pagerange_idx, base_page_idx, base_offset) = self.table.page_directory[rid]

            #Deleting Index of all columns of record after primary key
            # get base page we are working with
            pagerange = self.table.pagerange[pagerange_idx]

            # get current latest version of record
            current_page_idx = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset : base_offset + 4], 'big')
            current_byte_offset = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset + 4 : base_offset + 8], 'big')

            for i in range(self.table.num_columns):
                if i == 0:
                    continue
                col_idx = i + 4
                value_bytes = pagerange.array[col_idx][current_page_idx][current_byte_offset : current_byte_offset + 8]
                self.table.index.drop_index(i, str((int.from_bytes(value_bytes, 'big'))))

            self.table.page_directory[rid] = -1

            # mark record as deleted
            self.table.pagerange[pagerange_idx].page_to_num_records[base_page_idx] -= 1
        

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = int(0).to_bytes(8, 'big')
        
        if len(columns) != self.table.num_columns:
            return False

        # check if primary key already exists
        primary_key = columns[self.table.key]
        if self.table.index.locate(self.table.key, primary_key)[0]:
            print(f'Primary key: {primary_key} already exists.')
            return False
        
        # rid counter
        rid = self.table.rid_counter
        self.table.rid_counter += 1

        # This inserts the an index for the record into the b+tree.
        # self.table.index.create_index(self.table.key, primary_key, rid)
        # if self.table.rid_counter <= 2:
        #     print(self.table.key)
        #     print(primary_key)
        #     print(rid)
        #     print(" ")
        #  
        for i in range(len(columns)):
            self.table.index.create_index(i, columns[i], rid)

        # get page we are working with
        pagerange_idx = len(self.table.pagerange) - 1
        pagerange = self.table.pagerange[pagerange_idx]

        # if the last base page is full, make another one
        use_page_idx = pagerange.base_page_idxs[-1]
        if not pagerange.has_capacity(use_page_idx):
            self.table.new_pages(pagerange_idx, base_page=True)

            # update page we are working with
            pagerange_idx = len(self.table.pagerange) - 1
            pagerange = self.table.pagerange[pagerange_idx]
            use_page_idx = pagerange.base_page_idxs[-1]

        # user data
        for i in range(len(columns)):
            col_idx = i + 4

            # get offset index
            byte_offset = pagerange.page_to_num_records[use_page_idx] * pagerange.data_size
            data_size = pagerange.data_size

            # set data
            pagerange.array[col_idx][use_page_idx][byte_offset : byte_offset + 8] = columns[i].to_bytes(8, 'big')

        
        self.table.page_directory[rid] = (pagerange_idx, use_page_idx, byte_offset)
        indirection_value = (use_page_idx, byte_offset)
        pagerange.page_to_num_records[use_page_idx] += 1

        # metadata
        pagerange.array[RID_COLUMN][use_page_idx][byte_offset : byte_offset + 8] = rid.to_bytes(8, 'big')
        pagerange.array[TIMESTAMP_COLUMN][use_page_idx][byte_offset : byte_offset + 8] = int(datetime.now().timestamp()).to_bytes(8, 'big')
        pagerange.array[SCHEMA_ENCODING_COLUMN][use_page_idx][byte_offset : byte_offset + 8] = schema_encoding

        # indirection column stores two integers back to back
        pagerange.array[INDIRECTION_COLUMN][use_page_idx][byte_offset : byte_offset + 4] = indirection_value[0].to_bytes(4, 'big')
        pagerange.array[INDIRECTION_COLUMN][use_page_idx][byte_offset + 4 : byte_offset + 8] = indirection_value[1].to_bytes(4, 'big')

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
            # make sure rid exists in the table and is not deleted
            
            if rid == -1 or rid not in self.table.page_directory:
                continue
                
                
            # get initial version of the record
            (pagerange_idx, page_idx, offset) = self.table.page_directory[rid]
            column_values = []
            
            # get base page we are working with
            pagerange = self.table.pagerange[pagerange_idx]
            self.table.db.use_bufferpool(pagerange)    
            
            # get latest version
            page_idx_latest = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][page_idx][offset : offset + 4], 'big')
            byte_offset_latest = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][page_idx][offset + 4 : offset + 8], 'big')
            
            # get values of each column if it is in the query_columns
            for i in range(self.table.num_columns):
                col_idx = i + 4

                if query_columns[i]:
                    # get indirection value
                    value_bytes = pagerange.array[col_idx][page_idx_latest][byte_offset_latest : byte_offset_latest + 8]
                    column_values.append(int.from_bytes(value_bytes, 'big'))

                else:
                    column_values.append(None)

            record = Record(rid, self.table.key, column_values)
            results.append(record)
        return results


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # get rid from index
        rid = self.table.index.locate(self.table.key, primary_key)[0]

        # if rid doesn't exist or is deleted
        if rid == -1 or rid not in self.table.page_directory:
            return False

        if self.table._update_record(rid, columns):
            self.num_updates += 1
            if self.num_updates >= 500:
                (pagerange_idx, _, _) = self.table.page_directory[rid]
                self.table._merge(pagerange_idx)
            
            return True
        
        return False


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        #use this function for getting a list of RIDS within the begin and end range: locate_range(self, column, begin, end)
        rids = self.table.index.locate_range(self.table.key, start_range, end_range)
        # rids = self.table.index.locate_range(aggregate_column_index, start_range, end_range)

        if len(rids) == 0:
            return False

        total_sum = 0
        for rid in rids:
            # make sure record hasnt been deleted
            if rid == -1 or rid not in self.table.page_directory:
                continue

            (pagerange_idx, base_page_idx, base_offset) = self.table.page_directory[rid]
            pagerange = self.table.pagerange[pagerange_idx]
            pagerange = self.table.pagerange[pagerange_idx]
            self.table.db.use_bufferpool(pagerange) 
            # get latest version
            latest_page_idx = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset : base_offset + 4], 'big')
            byte_latest_offset = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset + 4 : base_offset + 8], 'big')

            value_bytes = pagerange.array[aggregate_column_index + 4][latest_page_idx][byte_latest_offset : byte_latest_offset + 8]
            total_sum += int.from_bytes(value_bytes, 'big')

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
