from logging import error
import sys


class Page:

    def __init__(self):
        self.capacity = 4096    # in bytes
        self.page_to_num_records = [0]
        self.path = None
        self.base_page_idxs = []
        self.tail_page_idxs = []
        self.data_size = 8      # offset per element in bytearray
        self.array = []
        self.pages = 0
        

    def has_capacity(self, page_idx):
        byte_end_idx = (self.page_to_num_records[page_idx] * self.data_size) + self.data_size
        if byte_end_idx < 4096:
            return True
        
        return False
