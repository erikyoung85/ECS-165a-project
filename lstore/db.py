from msilib.schema import File
from lstore.table import Table
from lstore.page import Page
import json
import os
class Database():

    def __init__(self):
        self.tables = []
        self.read = None
        self.path = None
        self.write = None
        self.bufferpool = []
        self.dirty = []
        self.bufferpool_limit = 32
        pass

    # Not required for milestone1
    def open(self, path):
        if (not os.path.isfile(path)):            
            f = open(path, 'w')
            self.path = path
            self.read = open(path, 'r')
        else:
            self.read = open(path, 'r')
            self.path = path
            num_tables = int(self.read.readline())
            for i in range(num_tables):
                info = self.read.readline().split()
                """
                0: name
                1: num_columns
                2: key
                3: page_directory path
                4: num_pageranges
                """
                #load main attributes to table
                table = Table(info[0], int(info[1]), int(info[2]))
                print(info[3])
                with open(info[3]) as f:
                    data = f.read()
                table.page_directory = json.loads(data)
                
                #load all pages to table [4] num_pageranges
                for j in range(info[4]):
                    path_pagerange = self.read.readline().strip
                    read_pagerange = open(path_pagerange, 'r')
                    pagerange = Page()
                    pagerange.path = read_pagerange.readline().strip()
                    pagerange.page_to_num_records = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.base_page_idxs = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.tail_page_idxs = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.data_size = int(read_pagerange.readline())
                    
                    #load data from array - not finished
                    for col in range(table.num_columns+4):
                        pagerange.array[col]
                    
                    pagerange.pages = int(read_pagerange.readline())
                
                self.tables.append(table)


    def close(self):
        f = open(self.path, 'w')
        f.write(str(len(self.tables))+"\n")
        for i in range(len(self.tables)):
            table : Table = self.tables[i]
            page_directory_path = table.name + "_" + "page_directory.json"
            with open(page_directory_path, 'w') as file:
                json.dump(table.page_directory, file)

            info = table.name + " " + str(table.num_columns) + " " + str(table.key) + " " + page_directory_path + " " + str(len(table.pagerange)) + "\n"
            f.write(info)
            
            for i in table.pagerange:
                f.write(i.path)
                
        for i in self.bufferpool:
            self.write_pagerange(i)
        
        f.close()
        self.read.close()

    def use_bufferpool(self, pagerange):
        if (len(self.bufferpool) < self.bufferpool_limit):
            self.bufferpool.append(pagerange)
            self.dirty.append(False)
            return True
        elif (len(self.bufferpool) == self.bufferpool_limit):
            for i in range(len(self.bufferpool)):
                if (not self.dirty[i]):
                    self.dirty.pop(0)
                    self.bufferpool.pop(0)
                    
                    self.dirty.append(False)
                    self.bufferpool.append(pagerange)
                    
                    return True
        return False
    
    def write_pagerange(self, pagerange : Page):
        path = pagerange.path
        file = open(path, 'w')        
        file.write(path)
        self.write_list(file, pagerange.page_to_num_records)
        self.write_list(file, pagerange.base_page_idxs)
        self.write_list(file, pagerange.tail_page_idxs)
        file.write(pagerange.data_size)
        
        #write data onto file
        
        file.write(str(pagerange.pages))
        
    def write_list(self, f : File, list):
        str = ""
        for i in list:
            str = str + " " + str(i)
        f.write(str)
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                self.tables.pop(i)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                return self.tables[i]
