from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        self.page = Page()
        self.basepage(name)

        # keep track of total records to create the next rid
        self.rid_counter = 0


# this function creates one page per column when the table is first created
# each column creates a list in the page.data array in the Page class
# each new page is appended to the corresponding list in page.array
# each page is just the bytearray(4096)

    def basepage(self, name):
        # create list for each column to hold base and tail pages
        for x in range(0, self.num_columns + 4):
            self.page.array.append([])
        
        # make initial base pages for each column
        self.new_pages()

# this appends a bytearray(4096) to the list of a specific column.
# use this when the array you are trying to input infor into gets full

    def new_pages(self):
        for i in range(self.num_columns + 4):
            self.page.array[i].append(bytearray(4096))

        self.page.pages += 1
        self.page.page_to_num_records.append(0)


    def __merge(self):
        print("merge is happening")
        pass
 
