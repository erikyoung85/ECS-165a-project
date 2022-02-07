import sys


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.array = []
        self.pages = 0
        self.cell_length = 4

# not sure if this works correctly but it should just check if the bytearray(4096) had an index error, and if
# so then it just creates a new page within that columns list
    def has_capacity(self,col):
        if IndexError:
            self.array[col].append(self.data)

    def write(self, value, offset):
        self.num_records += 1




