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
        self.num_records = 0


# this function creates one page per column when the table is first created
# each column creates a list in the page.data array in the Page class
# each new page is appended to the corresponding list in page.array
# each page is just the bytearray(4096)

    def basepage(self, name):
        self.page.pages = 4 + self.num_columns
        for x in range(0, self.page.pages):
            self.page.array.append([])
            self.page.array[x].append(self.page.data)

# this appends a bytearray(4096) to the list of a specific column.
# use this when the array you are trying to input infor into gets full

    def new_page(self, col):
        self.page.array[col].append(self.page.data)

    def __merge(self):
        print("merge is happening")
        pass
 
