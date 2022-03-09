from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from queue import LifoQueue

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.queryStack = LifoQueue()
        self.originalStack = LifoQueue()
        self.tableName = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.tableName == None:
            self.tableName = table
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        #print("query ran")
        for query, args in self.queries:
            # This segment saves original values if it encounters an update operation and finds existing records
            if len(args) == 2:
                saveOrignal = Query(self.tableName).currentValues(args[0])
                if len(saveOrignal) != 0:
                    self.originalStack.put(saveOrignal)

            # This segment gets original RID to be put into stack (not for insert)
            if len(args) != 5:
                originalRID = Query(self.tableName).returnRID(args[0])

            # Runs the operation
            result = query(*args)

            # This segment gets original RID to be put into stack (for insert)
            if len(args) == 5:
                originalRID = Query(self.tableName).returnRID(args[0])
            
            # If op fails, it aborts
            if result == False:
                return self.abort()
            
            # Stack for aborting is appended if needed
            self.queryStack.put((query, args, originalRID))
            #print("stack appended")

        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        
        #print("aborting")
        
        #If stack of ops that went through is not empty, reverse the damage
        while self.queryStack.empty() == False:
            #print("in loop")
            revQuery = self.queryStack.get()
            print(self.queryStack.qsize())

            # This skips select ops
            if len(revQuery[1]) == 3:
                continue

            # This undos insert with a delete
            if len(revQuery[1]) == 5:    
                Query(self.tableName).delete(revQuery[1][0])
                print("successful insert abort")
                continue

            # This undos update with an update of the original values
            if len(revQuery[1]) == 2:    
                Query(self.tableName).update(revQuery[1][0], self.originalStack.get())
                print("successful update abort")

        while not self.originalStack.empty():
            self.originalStack.get() 
        return False

    def commit(self):
        # TODO: commit to database
        #print("query committed")

        # Emptying out the stacks
        while not self.queryStack.empty():
            self.queryStack.get()
        while not self.originalStack.empty():
            self.originalStack.get()
        return True
