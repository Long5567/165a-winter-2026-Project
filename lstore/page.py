import struct
from lstore.config import PAGE_SIZE

INT_SIZE = 8  # each record is a 64-bit integer in bytes
MAX_RECORDS_PER_PAGE = PAGE_SIZE // INT_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    # To check if there has capacity for the physical page
    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    # To implement the write page function
    def write(self, value):
        if not self.has_capacity():
            return None
        offset = self.num_records
        if value is not None:
            struct.pack_into(">q", self.data, offset * INT_SIZE, value)
        self.num_records += 1
        return offset
    
    # To implement the page read function
    def read(self, offset):
        if offset >= self.num_records:
            return None
        return struct.unpack_from(">q", self.data, offset * INT_SIZE)[0]

    # To implement the page update function 
    def update(self, offset, value):
        if offset >= self.num_records:
            return None
        if value is not None:
            struct.pack_into(">q", self.data, offset * INT_SIZE, value)
        else: 
            self.data[offset * INT_SIZE : offset * INT_SIZE + INT_SIZE] = bytearray(INT_SIZE)
        return True

