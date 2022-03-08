from lstore.table import Table, Record
from lstore.index import Index
from collections import deque

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.queryStack = deque()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, table, query, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            self.queryStack.append((query, args))
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # Notes for update and delete: if this operation is popped, the next pop will be a select operation. this can be used to find the key
        revQuery = self.queryStack.pop()
        #if revQuery == q.update:
            #selQuery = self.queryStack.pop()
        return False

    def commit(self):
        # TODO: commit to database
        return True

