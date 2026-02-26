from lstore.table import Table
from lstore.disk_manager import DiskManager
from lstore.bufferpool import BufferPool
from lstore.config import BUFFERPOOL_SIZE, PAGE_SIZE
import os


class Database():
    def __init__(self):
        self.tables = []
        self.disk_manager = None
        self.bufferpool = None
    # Not required for milestone1
    def open(self, path):
        self.disk_manager = DiskManager(path)
        self.bufferpool = BufferPool(self.disk_manager, BUFFERPOOL_SIZE)

        # To begin with a new data base if the Data Base directory is non-existing.
        if not os.path.exists(path):
            return
        for table_name in os.listdir(path):
            table_path = os.path.join(path, table_name)
            if not os.path.isdir(table_path):
                continue
            meta_path = os.path.join(table_path, "metadata.txt")
            if not os.path.exists(meta_path):
                continue
            self.load_table(table_name)
    def close(self):
        if self.bufferpool is not None:
            self.bufferpool.flush_all()
        for table in self.tables:
            table.save_metadata(self.disk_manager)   # Change to new lightweight method
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        existing = self.get_table(name)
        if existing is not None:
            return existing
        table = Table(name, num_columns, key_index)
        table.bind_storage(self.bufferpool, self.disk_manager)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            cur_table = self.tables[i]
            if cur_table.name == name:
                self.tables.remove(cur_table)
                return True
        return False

    """
    # To return table with the passed name
    """
    def get_table(self, name):
        # Return the most recently created / loaded table 
        for table in reversed(self.tables):
            if table.name == name:
                return table
        return None

    def load_table(self, table_name):
        table_path = os.path.join(self.disk_manager.path, table_name)
        meta_path = os.path.join(table_path, "metadata.txt")
        if not os.path.exists(meta_path):
            return
        f = open(meta_path, "r")
        num_columns = int(f.readline())
        key = int(f.readline())
        next_base_rid = int(f.readline())
        next_tail_rid = int(f.readline())
        f.close()
        table = Table(table_name, num_columns, key)
        table.bind_storage(self.bufferpool, self.disk_manager)
        table.next_base_rid = next_base_rid
        table.next_tail_rid = next_tail_rid
        records_per_page = PAGE_SIZE // 8

        # To locate existing base page slots from disk
        # not reading all page from memory
        base_path = os.path.join(table_path, "base")
        if os.path.exists(base_path):
            for col in os.listdir(base_path):
                col_path = os.path.join(base_path, col)
                if not os.path.isdir(col_path):
                    continue
                col_index = int(col)
                for file in os.listdir(col_path):
                    if not file.endswith(".bin"):
                        continue
                    page_index = int(file.replace(".bin", ""))
                    while len(table.base_pages[col_index]) <= page_index:
                        table.base_pages[col_index].append(None)
        for col in range(table.total_columns):
            table.current_base_page_index[col] = max(0, len(table.base_pages[col]) - 1)

        # finding existing base page slots from disk
        # not reading all page from memory
        tail_path = os.path.join(table_path, "tail")
        if os.path.exists(tail_path):
            for col in os.listdir(tail_path):
                col_path = os.path.join(tail_path, col)
                if not os.path.isdir(col_path):
                    continue
                col_index = int(col)
                for file in os.listdir(col_path):
                    if not file.endswith(".bin"):
                        continue
                    page_index = int(file.replace(".bin", ""))
                    while len(table.tail_pages[col_index]) <= page_index:
                        table.tail_pages[col_index].append(None)
        for col in range(table.total_columns):
            table.current_tail_page_index[col] = max(0, len(table.tail_pages[col]) - 1)

        pd_path = os.path.join(table_path, "page_directory.txt")
        if os.path.exists(pd_path):
            range_to_tail_pages = {}
            f = open(pd_path, "r")
            for raw_line in f:
                line = raw_line.strip()
                if line == "":
                    continue
                split_idx = line.find("|")
                if split_idx == -1:
                    continue
                rid = int(line[:split_idx])
                payload = line[split_idx + 1:]
                entries = []
                for token in payload.split(";"):
                    fields = token.split(",")
                    if len(fields) != 5:
                        continue
                    mark = fields[0]
                    col = int(fields[1])
                    range_index = int(fields[2])
                    page_index = int(fields[3])
                    offset_raw = int(fields[4])
                    offset = None if offset_raw < 0 else offset_raw
                    entries.append((mark, col, range_index, page_index, offset))
                if len(entries) != table.total_columns:
                    continue
                table.page_directory[rid] = entries
                if rid > 0:
                    table.base_rids.add(rid)
                else:
                    rid_entry = entries[1]
                    range_to_tail_pages.setdefault(rid_entry[2], set()).add(rid_entry[3])
            f.close()

            tps_path = os.path.join(table_path, "tps.txt")
            if os.path.exists(tps_path):
                f = open(tps_path, "r")
                for raw_line in f:
                    line = raw_line.strip()
                    if line == "":
                        continue
                    split_idx = line.find("|")
                    if split_idx == -1:
                        continue
                    rid = int(line[:split_idx])
                    value_str = line[split_idx + 1:]
                    table.tps[rid] = None if value_str == "N" else int(value_str)
                f.close()

            star_path = os.path.join(table_path, "star_tail.txt")
            if os.path.exists(star_path):
                f = open(star_path, "r")
                for raw_line in f:
                    line = raw_line.strip()
                    if line == "":
                        continue
                    table.star_tail_record.add(int(line))
                f.close()

            for rid in sorted(table.base_rids):
                base_record = table.read_record(rid)
                if base_record is None:
                    continue
                key_value = base_record[4 + table.key]
                if key_value is not None:
                    table.index.insert_key(key_value, rid)

            table._register_existing_tail_pages(range_to_tail_pages)
            self.tables.append(table)
            return

        # To rebuild base page_directory and primary-key index
        for rid in range(1, table.next_base_rid):
            offset = rid - 1
            page_index = offset // records_per_page
            offset_in_page = offset % records_per_page

            if page_index >= len(table.base_pages[1]):
                continue
            rid_value = table._read_cell(False, 1, page_index, offset_in_page)
            if rid_value is None or rid_value == 0:
                continue

            directory = []
            indirection_value = table._read_cell(False, 0, page_index, offset_in_page)
            range_index = table._base_range_from_page_index(page_index)

            for col in range(table.total_columns):
                if col == 0:
                    if indirection_value is None or indirection_value == 0:
                        directory.append(('N', col, range_index, page_index, offset_in_page))
                    else:
                        directory.append(('B', col, range_index, page_index, offset_in_page))
                else:
                    directory.append(('B', col, range_index, page_index, offset_in_page))

            table.page_directory[rid] = directory
            table.base_rids.add(rid)

            key_col = 4 + table.key
            key_value = table._read_cell(False, key_col, page_index, offset_in_page)
            if key_value is not None:
                table.index.insert_key(key_value, rid)

        # To rebuild tail page_directory from RID value in the disk
        max_se = (1 << table.num_columns) - 1
        tail_entries = []
        rid_pages = table.tail_pages[1]
        for page_index in range(len(rid_pages)):
            rid_frame = table._fetch_frame(True, 1, page_index, pin=True)
            if rid_frame is None:
                continue
            try:
                num_records = rid_frame.num_records
            finally:
                table._unpin(True, 1, page_index)

            for offset_in_page in range(num_records):
                rid = table._read_cell(True, 1, page_index, offset_in_page)
                if rid is None or rid == 0:
                    continue
                indirection_value = table._read_cell(True, 0, page_index, offset_in_page)
                se = table._read_cell(True, 3, page_index, offset_in_page)
                if se is None:
                    se = 0
                tail_entries.append((rid, page_index, offset_in_page, indirection_value, se))

        tail_entries.sort(key=lambda x: x[0], reverse=True)
        tail_range = {}
        range_to_tail_pages = {}
        for rid, page_index, offset_in_page, indirection_value, se in tail_entries:
            if indirection_value is None or indirection_value == 0:
                range_index = 0
            elif indirection_value > 0:
                base_dir = table.page_directory.get(indirection_value)
                if base_dir is not None:
                    range_index = base_dir[1][2]
                else:
                    range_index = table._base_range_from_rid(indirection_value)
            else:
                range_index = tail_range.get(indirection_value)
                if range_index is None:
                    prev_dir = table.page_directory.get(indirection_value)
                    if prev_dir is not None:
                        range_index = prev_dir[1][2]
                    else:
                        range_index = 0
            tail_range[rid] = range_index
            range_to_tail_pages.setdefault(range_index, set()).add(page_index)

            directory = []
            if indirection_value is None or indirection_value == 0:
                directory.append(('N', 0, range_index, page_index, offset_in_page))
            else:
                directory.append(('T', 0, range_index, page_index, offset_in_page))
            directory.append(('T', 1, range_index, page_index, offset_in_page))
            directory.append(('T', 2, range_index, page_index, offset_in_page))
            directory.append(('T', 3, range_index, page_index, offset_in_page))

            for j in range(table.num_columns):
                bit = 1 << (table.num_columns - 1 - j)
                col_index = 4 + j
                if (se & bit) == 0:
                    directory.append(('N', col_index, range_index, page_index, offset_in_page))
                else:
                    directory.append(('T', col_index, range_index, page_index, offset_in_page))

            table.page_directory[rid] = directory
            if se == max_se and indirection_value is not None and indirection_value > 0:
                table.star_tail_record.add(rid)

        table._register_existing_tail_pages(range_to_tail_pages)

        self.tables.append(table)
