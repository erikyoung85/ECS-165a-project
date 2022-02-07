import sys


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.array = []
        self.pages = 0
        self.capacity = 1024

# not sure if this works correctly but it should just check if the bytearray(4096) had an index error, and if
# so then it just creates a new page within that columns list
    def has_capacity(self, page_idx):
        if len(self.array[0][page_idx]) < self.capacity:
            return True
        
        return False

    def write(self, value):
        self.num_records += 1




