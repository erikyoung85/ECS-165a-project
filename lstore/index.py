from lstore.bPlusTree import bPlusTree, Node

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        #self.indices = [None] *  table.num_columns
        self.indicesRange = []
        #self.indices = [bPlusTree(10)] * table.num_columns
        self.indices = bPlusTree(10)
        pass

    """
    # returns the location of all records with the given value on column "column"
    # Uses B+Tree to search for the RID that corresponds with the given key
    """

    def locate(self, column, value):
        RIDs = []
        correctRID = None
        RIDsList = self.indices.searchRID(str(value))
        for i in range(len(RIDsList)):
            if str(value) == RIDsList[i][0][0]:
                correctRID = RIDsList[i][0][1]
                break
        RIDs.append(correctRID)
        #print(type(correctRID))
        #print(value)
        #print(RIDs[0])
        return RIDs
        #pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        RIDs = []
        for i in range(len(self.indicesRange)): 
            if column == self.indicesRange[i][0] and begin >= self.indicesRange[i][1] and end <= self.indicesRange[i][1]:
                RIDs.append(self.indices[i][2])
        return RIDs
        #pass

    """
    # optional: Create index on specific column
    # Creates an index for the B+Tree and the list for indicesRange
    """

    #def sortKeyValue(e):
    #    return e[1]

    def create_index(self, column_number, key, RID):    # (self, column_number):
        #print(key)
        #print(RID)
        self.indicesRange.append([column_number, key, RID])
        self.indices.insert(str(key), [str(key), RID])
        pass

    """
    # optional: Drop index of specific column
    """
    
    #Deletes index from B+Tree
    def drop_index(self, column_number, key):
        RIDtoDelete = self.locate(column_number, str(key))
        #print(key)
        #print(RIDtoDelete)
        #self.indices.delete(str(key), [str(key), RIDtoDelete[0]])
        self.indices.reserveRID(str(key))
        for i in range(len(self.indicesRange)): 
            if key == self.indicesRange[i][1] and column_number == self.indicesRange[i][0]:
                self.indicesRange[i][2] = -1
                break
        return RIDtoDelete
