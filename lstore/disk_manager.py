import os
from lstore.config import PAGE_SIZE

class DiskManager():
    """
    DiskManager functions to provide a persistence layer for L-Store pages.
    
    - The function stores raw page bytes in .bin, and store valid records numbers in .cnt.
    """
    
    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)

    def write_page(self, table_name, is_tail, column, page_index, data, num_records):
        table_path = os.path.join(self.path, table_name)
        page_type = "tail" if is_tail else "base"
        type_path = os.path.join(table_path, page_type)
        col_path = os.path.join(type_path, str(column))
        os.makedirs(col_path, exist_ok=True)

        # raw page bytes
        file_path = os.path.join(col_path, str(page_index) + ".bin")
        payload = bytes(data)
        if len(payload) < PAGE_SIZE:
            payload = payload + bytes(PAGE_SIZE - len(payload))
        elif len(payload) > PAGE_SIZE:
            payload = payload[:PAGE_SIZE]
        f = open(file_path, "wb")
        f.write(payload)
        f.close()

        # To record count metadata
        cnt_path = os.path.join(col_path, str(page_index) + ".cnt")
        f = open(cnt_path, "w")
        f.write(str(num_records))
        f.close()

    def read_page(self, table_name, is_tail, column, page_index):
        page_type = "tail" if is_tail else "base"
        file_path = os.path.join(
            self.path,
            table_name,
            page_type,
            str(column),
            str(page_index) + ".bin"
        )

        if not os.path.exists(file_path):
            return None

        f = open(file_path, "rb")
        data = f.read()
        f.close()
        return data

    def read_page_count(self, table_name, is_tail, column, page_index):
        page_type = "tail" if is_tail else "base"
        cnt_path = os.path.join(
            self.path,
            table_name,
            page_type,
            str(column),
            str(page_index) + ".cnt"
        )

        if not os.path.exists(cnt_path):
            return 0

        f = open(cnt_path, "r")
        s = f.readline()
        f.close()

        if s is None or s == "":
            return 0
        return int(s)

    def delete_page(self, table_name, is_tail, column, page_index):
        page_type = "tail" if is_tail else "base"
        base = os.path.join(
            self.path,
            table_name,
            page_type,
            str(column),
            str(page_index),
        )
        file_path = base + ".bin"
        cnt_path = base + ".cnt"
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(cnt_path):
            os.remove(cnt_path)
        return True
