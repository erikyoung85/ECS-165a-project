from lstore.table import Table
from lstore.page import Page
import json
import os
import time, threading

def int_keys(ordered_pairs):
        result = {}
        for key, value in ordered_pairs:
            try:
                key = int(key)
            except ValueError:
                pass
            result[key] = value
        return result
    
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
                table.db = self
                with open(info[3]) as f:
                    data = f.read()
                table.page_directory = json.loads(data, object_pairs_hook=int_keys)
                
                table.pagerange = []
                #load all pages to table [4] num_pageranges
                for j in range(int(info[4])):
                    path_pagerange = self.read.readline().strip()
                    read_pagerange = open(path_pagerange, 'r')
                    pagerange = Page()
                    pagerange.path = read_pagerange.readline().strip()
                    pagerange.page_to_num_records = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.base_page_idxs = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.tail_page_idxs = [int(num) for num in read_pagerange.readline().strip().split()]
                    pagerange.data_size = int(read_pagerange.readline())                 
                    pagerange.pages = int(read_pagerange.readline())
                    binary_path = read_pagerange.readline().strip()
                    #load data from array - not finished
                    binary_file = open(binary_path, 'rb')
                    for col in range(table.num_columns+4):
                        array = []
                        for k in range(pagerange.pages):
                            array.append(bytearray(binary_file.read(4096)))                        
                        pagerange.array.append(array)
                    binary_file.close()
                    table.pagerange.append(pagerange)
                    table.index.all_index()
                self.tables.append(table)
                
    def pagerange_in_bufferpool(self, pagerange): 
        for i in range(len(self.bufferpool)):
            if (pagerange.path == self.bufferpool[i].path):
                return i
        
        return -1

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
                f.write(i.path+"\n")
            
                
        for i in self.bufferpool:
            self.write_pagerange(i)
        
        f.close()
        self.read.close()

    """add a new pagerange to the bufferpool
    if all pagerange are in transaction, return False
    if already exists in bufferpool, return False
    otherwise, append new pagerange and return True
    """
    def use_bufferpool(self, pagerange):
        if not self.pagerange_in_bufferpool(pagerange) == -1:
            return False
        if (len(self.bufferpool) < self.bufferpool_limit):
            self.bufferpool.append(pagerange)
            self.dirty.append(False)
            return True
        elif (len(self.bufferpool) == self.bufferpool_limit):
            for i in range(len(self.bufferpool)):
                if (True): #this should check if any transaction uses the current page
                    dirty = self.dirty.pop(0)
                    evict_page  = self.bufferpool.pop(0)
                    
                    #write the evicted page onto the file
                    if dirty:
                        self.write_pagerange(evict_page)
                    
                    self.dirty.append(False)
                    self.bufferpool.append(pagerange)
                    
                    return True
        return False

    
    def write_pagerange(self, pagerange : Page):
        path = pagerange.path
        file = open(path, 'w')        
        file.write(path+"\n")
        self.write_list(file, pagerange.page_to_num_records)
        self.write_list(file, pagerange.base_page_idxs)
        self.write_list(file, pagerange.tail_page_idxs)
        file.write(str(pagerange.data_size)+"\n")        
        file.write(str(pagerange.pages)+"\n")
        
        #write array onto file - not finished
        binary_path = path + " binary_file"
        binary_file = open(binary_path, 'wb')
        for i in range(len(pagerange.array) ):
            for j in pagerange.array[i]:
                binary_file.write(j)
        binary_file.close()
        file.write(binary_path)
        file.close()
        
    def write_list(self, f, list):
        s = ""
        for i in list:
            s = s + " " + str(i)
        s = s + "\n"
        f.write(s)
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.db = self
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
