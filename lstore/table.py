from lstore.index import Index
from lstore.page import Page
import time, threading
from datetime import datetime
import copy

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    
    def __str__(self) -> str:
        return self.rid + " " + self.key
class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        # self.page = Page()
        page = Page()
        page.path = self.name + " page_range 0" 
        self.pagerange = [page]
        self.pagerange_capacity = 16
        self.basepage(pagerange_idx=0)
        self.db = None
        # keep track of total records to create the next rid
        self.rid_counter = 1


# this function creates one page per column when the table is first created
# each column creates a list in the page.data array in the Page class
# each new page is appended to the corresponding list in page.array
# each page is just the bytearray(4096)

    def basepage(self, pagerange_idx):
        # create list for each column to hold base and tail pages
        for x in range(0, self.num_columns + 4):
            self.pagerange[pagerange_idx].array.append([])
        
        # make initial base pages for each column
        self.new_pages(pagerange_idx, base_page=True)

# this appends a bytearray(4096) to the list of a specific column.
# use this when the array you are trying to input infor into gets full

    def new_pages(self, pagerange_idx, base_page: bool):
        num_full_basepages = len(self.pagerange[pagerange_idx].base_page_idxs)
        should_create_pagerange = num_full_basepages >= self.pagerange_capacity and base_page == True

        if should_create_pagerange:
            # create new pagerange
            page = Page()
            page.path = self.name + " page_range " + str(len(self.pagerange) )
            self.pagerange.append(page)
            pagerange_idx += 1

            # create list for each column to hold base and tail pages
            for x in range(0, self.num_columns + 4):
                self.pagerange[pagerange_idx].array.append([])


        # get pagerange that we are working with
        pagerange = self.pagerange[pagerange_idx]

        for i in range(self.num_columns + 4):
            pagerange.array[i].append(bytearray(4096))

        pagerange.page_to_num_records.append(0)
        pagerange.pages += 1

        if base_page:
            pagerange.base_page_idxs.append(pagerange.pages - 1)
        else:
            pagerange.tail_page_idxs.append(pagerange.pages - 1)

    def __merge(self, pagerange_idx):
        new_pagerange = Page()
        self.pagerange.append(new_pagerange)
        new_pagerange_idx = len(self.pagerange) - 1

        # copy base pages
        for col_idx in range(self.num_columns + 4):
            for page_idx in self.pagerange[pagerange_idx].base_page_idxs:
                # copy each base page
                new_pagerange.array[col_idx].append(copy.deepcopy(self.pagerange[pagerange_idx].array[col_idx][page_idx]))
                new_page_idx = len(new_pagerange.array[col_idx]) - 1
                new_pagerange.base_page_idxs.append(new_page_idx)

        new_pagerange.pages = len(new_pagerange.array) - 1

        # update values
        for page_idx in range(new_pagerange.base_page_idxs):
            for byte_offset in range(0, 4086, 8):
                rid = int.from_bytes(new_pagerange.array[RID_COLUMN][page_idx][byte_offset : byte_offset + 8], 'big')
                schema_encoding = int.from_bytes(new_pagerange.array[SCHEMA_ENCODING_COLUMN][page_idx][byte_offset : byte_offset + 8], 'big')

                # set new indirection page_idx value
                new_pagerange.array[INDIRECTION_COLUMN][page_idx][byte_offset : byte_offset + 4] = page_idx.to_bytes(4, 'big')
                
                # update if necessary
                if schema_encoding != 0:
                    # get most updated values
                    new_values = self.latest_by_rid(rid)

                    # update page directory
                    self.page_directory[rid] = (new_pagerange_idx, new_page_idx, byte_offset)

                    # add latest updated columns update
                    self._update_record(rid, new_values)
                else:
                    # ONLY update page directory
                    self.page_directory[rid] = (new_pagerange_idx, new_page_idx, byte_offset)

        # once all done, set initial pagerange to NULL to save space
        self.pagerange[pagerange_idx] = None



    def _update_record(self, rid, columns):
        # get base record
        (pagerange_idx, base_page_idx, base_offset) = self.page_directory[rid]

        # get pagerange we are working with
        pagerange = self.pagerange[pagerange_idx]

        self.db.use_bufferpool(pagerange) 
        index_bufferpool = self.db.pagerange_in_bufferpool(pagerange)
        self.db.dirty[index_bufferpool] = True
        # get current latest version of record
        current_page_idx = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset : base_offset + 4], 'big')
        
        current_byte_offset = int.from_bytes(pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset + 4 : base_offset + 8], 'big')

        schema_encoding = "0" * self.num_columns

        # get base record's schema encoding
        base_schema_encoding_bytes = pagerange.array[SCHEMA_ENCODING_COLUMN][base_page_idx][base_offset : base_offset + 8]
        base_schema_encoding = self.convert_schema_encoding(base_schema_encoding_bytes)

        #Updating Index
        for i in range(self.num_columns):
            if columns[i] == None:
                continue
            col_idx = i + 4
            value_bytes = pagerange.array[col_idx][current_page_idx][current_byte_offset : current_byte_offset + 8]
            self.index.update_index(i, str((int.from_bytes(value_bytes, 'big'))), columns[i])

        # if there are no tail pages, or if the current tail page is full, make another one
        if not pagerange.tail_page_idxs or not pagerange.has_capacity(pagerange.tail_page_idxs[-1]):
            self.new_pages(pagerange_idx, base_page=False)
        
        # get page idx and 
        new_page_idx = pagerange.tail_page_idxs[-1]

        for i in range(len(columns)):

            col_idx = i + 4
            col_page_array = pagerange.array[col_idx]

            new_byte_offset = pagerange.page_to_num_records[new_page_idx] * pagerange.data_size

            current_value = int.from_bytes(pagerange.array[col_idx][current_page_idx][current_byte_offset : current_byte_offset + 8], 'big')
            new_value = columns[i]

            if columns[i] == None:
                # if column is keeping its current value
                pagerange.array[col_idx][new_page_idx][new_byte_offset : new_byte_offset + 8] = current_value.to_bytes(8, 'big')

            else:
                # if column has a new value
                schema_encoding = schema_encoding[:i] + "1" + schema_encoding[i+1:]
                base_schema_encoding = base_schema_encoding[:i] + "1" + base_schema_encoding[i+1:]
                pagerange.array[col_idx][new_page_idx][new_byte_offset : new_byte_offset + 8] = new_value.to_bytes(8, 'big')

        schema_encoding = int(schema_encoding, 2).to_bytes(8, 'big')
        base_schema_encoding = int(base_schema_encoding, 2).to_bytes(8, 'big')

        # set metadata for updated record
        pagerange.array[RID_COLUMN][new_page_idx][new_byte_offset : new_byte_offset + 8] = rid.to_bytes(8, 'big')
        pagerange.array[TIMESTAMP_COLUMN][new_page_idx][new_byte_offset : new_byte_offset + 8] = int(datetime.now().timestamp()).to_bytes(8, 'big')
        pagerange.array[SCHEMA_ENCODING_COLUMN][new_page_idx][new_byte_offset : new_byte_offset + 8] = schema_encoding

        pagerange.array[INDIRECTION_COLUMN][new_page_idx][new_byte_offset : new_byte_offset + 4] = current_page_idx.to_bytes(4, 'big')
        pagerange.array[INDIRECTION_COLUMN][new_page_idx][new_byte_offset + 4 : new_byte_offset + 8] = current_byte_offset.to_bytes(4, 'big')

        # set indirection column and schema encoding values for BASE PAGE
        pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset : base_offset + 4] = new_page_idx.to_bytes(4, 'big')
        pagerange.array[INDIRECTION_COLUMN][base_page_idx][base_offset + 4 : base_offset + 8] = new_byte_offset.to_bytes(4, 'big')
        pagerange.array[SCHEMA_ENCODING_COLUMN][base_page_idx][base_offset : base_offset + 8] = base_schema_encoding

        pagerange.page_to_num_records[new_page_idx] += 1

        return True


    def latest_by_rid(self, rid):

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

            value_bytes = pagerange.array[col_idx][page_idx_latest][byte_offset_latest : byte_offset_latest + 8]
            column_values.append(int.from_bytes(value_bytes, 'big'))

        return column_values
