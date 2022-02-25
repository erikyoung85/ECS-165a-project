from lstore.bPlusTree import bPlusTree, Node

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        #self.indices = [None] *  table.num_columns
        #self.indicesRange = []
        self.indices = []
        self.table = table
        #self.indices = [bPlusTree(10)] * table.num_columns
        #print (self.indices)
        #self.indices = bPlusTree(10)
        for i in range(self.table.num_columns):
            self.indices.append(bPlusTree(20))

    """
    # returns the location of all records with the given value on column "column"
    # Uses B+Tree to search for the RID that corresponds with the given key
    """

    def locate(self, column, value):
        RIDs = []
        correctRID = None
        RIDsList = self.indices[column].searchRID(str(value))
        for i in range(len(RIDsList)):
            if str(value) == RIDsList[i][0][0]:
                correctRID = RIDsList[i][0][1]
                break
        RIDs.append(correctRID)
        #print(type(correctRID))
        #print(value)
        #print(RIDs[0])
        return RIDs

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, column, begin, end):
        #print(column)
        return self.indices[column].searchRange(begin, end)

    """
    # optional: Create index on specific column
    # Creates an index for the B+Tree and the list for indicesRange
    """


    def create_index(self, column_number, key, RID):
        self.indices[column_number].insert(str(key), [str(key), RID])
        #if RID <= 2:
            #print(str(key))
            #print([str(key), RID])
            #print(" ")

    """
    # optional: Drop index of specific column
    """
    
    #Deletes index from B+Tree
    def drop_index(self, column_number, key):
        RIDtoDelete = self.locate(column_number, str(key))
        self.indices[column_number].reserveRID(str(key))
        return RIDtoDelete


    #Updating index
    def update_index(self, column, key, newKey):
        self.indices[column].updateKey(str(key), str(newKey))
        
    def all_index(self):
        for i in range(self.table.num_columns):
            self.indices.append(bPlusTree(20))
            
            for RID in self.table.page_directory:
                (pagerange_idx, base_page_idx, base_offset) = self.table.page_directory[RID]
                pagerange = self.table.pagerange[pagerange_idx]
                key = int.from_bytes(pagerange.array[i+4][base_page_idx][base_offset : base_offset + 8], 'big')
                self.indices[i].insert(str(key), [str(key), RID])
