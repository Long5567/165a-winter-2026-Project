from lstore.index import Index
from time import time
from lstore.config import PAGE_SIZE, BASE_PAGES_PER_RANGE, MERGE_TAIL_PAGE_THRESHOLD
import time
import os
import threading
import struct

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
INT_SIZE = 8
RECORDS_PER_PAGE = PAGE_SIZE // INT_SIZE

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        """
        page_directory is a dictionary. The key is the rid, and
        the value is a tuple (B or T, Column_Index, Range_Index, Page_Index, Offset)
        """
        self.page_directory = {}
        self.index = Index(self)
        self.merge_tail_page_threshold = MERGE_TAIL_PAGE_THRESHOLD
        
        self.total_columns = num_columns + 4 # first 4 col is for metadata
        # only tracking page slots 
        # physical page bytes are managed by bufferpool/disk
        self.base_pages = [[None] for _ in range(self.total_columns)]
        self.tail_pages = [[None] for _ in range(self.total_columns)]

        # tracking the special tail record with SE* when first time update the column
        self.star_tail_record = set()

        # Base rid starts from 1 and tail rid starts from -1
        self.next_base_rid = 1
        self.next_tail_rid = -1
        self.base_pages_per_range = BASE_PAGES_PER_RANGE
        self.records_per_range = self.base_pages_per_range * RECORDS_PER_PAGE
        # Tps
        self.tps = {}
        self.base_rids = set()
        self._sorted_base_rids_cache = None
        # each list stores global tail page indices
        self.tail_range_pages = {0: [[0] for _ in range(self.total_columns)]}

        self.current_base_page_index = []
        self.current_tail_page_index = []
        for i in range(self.total_columns):
            self.current_base_page_index.append(0)
            self.current_tail_page_index.append(0)
        self.bufferpool = None
        self.disk_manager = None
        self.latch = threading.RLock()
        self._tail_pages_created_since_merge = 0
        self._merge_request = threading.Event()
        self._merge_stop = threading.Event()
        self._pending_merge_jobs = []
        self._merge_thread = None

    def bind_storage(self, bufferpool, disk_manager):
        self.bufferpool = bufferpool
        self.disk_manager = disk_manager

    def _fetch_frame(self, is_tail, column, page_index, pin=True):
        if self.bufferpool is None:
            return None
        return self.bufferpool.fetch_page(self.name, is_tail, column, page_index, pin=pin)

    def _unpin(self, is_tail, column, page_index):
        if self.bufferpool is None:
            return
        self.bufferpool.unpin_page(self.name, is_tail, column, page_index)

    def _read_cell(self, is_tail, column, page_index, offset):
        frame = self._fetch_frame(is_tail, column, page_index, pin=True)
        if frame is None:
            return None
        try:
            if offset is None or offset < 0 or offset >= frame.num_records:
                return None
            return struct.unpack_from(">q", frame.data, offset * INT_SIZE)[0]
        finally:
            self._unpin(is_tail, column, page_index)

    def _append_cell(self, is_tail, column, page_index, value):
        frame = self._fetch_frame(is_tail, column, page_index, pin=True)
        if frame is None:
            return None
        try:
            if len(frame.data) < PAGE_SIZE:
                frame.data.extend(bytearray(PAGE_SIZE - len(frame.data)))
            if frame.num_records >= RECORDS_PER_PAGE:
                return None
            offset = frame.num_records
            if value is None:
                frame.data[offset * INT_SIZE: offset * INT_SIZE + INT_SIZE] = bytearray(INT_SIZE)
            else:
                struct.pack_into(">q", frame.data, offset * INT_SIZE, value)
            frame.num_records += 1
            self.bufferpool.mark_dirty(self.name, is_tail, column, page_index)
            return offset
        finally:
            self._unpin(is_tail, column, page_index)

    def _update_cell(self, is_tail, column, page_index, offset, value):
        frame = self._fetch_frame(is_tail, column, page_index, pin=True)
        if frame is None:
            return False
        try:
            if len(frame.data) < PAGE_SIZE:
                frame.data.extend(bytearray(PAGE_SIZE - len(frame.data)))
            if offset is None or offset < 0 or offset >= frame.num_records:
                return False
            if value is None:
                frame.data[offset * INT_SIZE: offset * INT_SIZE + INT_SIZE] = bytearray(INT_SIZE)
            else:
                struct.pack_into(">q", frame.data, offset * INT_SIZE, value)
            self.bufferpool.mark_dirty(self.name, is_tail, column, page_index)
            return True
        finally:
            self._unpin(is_tail, column, page_index)

    def _page_has_capacity(self, is_tail, column, page_index):
        frame = self._fetch_frame(is_tail, column, page_index, pin=True)
        if frame is None:
            return False
        try:
            return frame.num_records < RECORDS_PER_PAGE
        finally:
            self._unpin(is_tail, column, page_index)

    def _base_range_from_page_index(self, page_index):
        return page_index // self.base_pages_per_range

    def _base_range_from_rid(self, base_rid):
        if base_rid is None or base_rid <= 0:
            return 0
        return (base_rid - 1) // self.records_per_range

    def _get_base_range_for_rid(self, base_rid):
        directory = self.page_directory.get(base_rid)
        if directory is not None:
            return directory[RID_COLUMN][2]
        return self._base_range_from_rid(base_rid)

    def _ensure_tail_range(self, range_index):
        if range_index not in self.tail_range_pages:
            self.tail_range_pages[range_index] = [[] for _ in range(self.total_columns)]

    def _register_existing_tail_pages(self, range_to_pages):
        self.tail_range_pages = {}
        if not range_to_pages:
            self.tail_range_pages[0] = [[0] for _ in range(self.total_columns)]
            self._tail_pages_created_since_merge = 0
            return
        for range_index, pages in range_to_pages.items():
            sorted_pages = sorted(pages)
            self.tail_range_pages[range_index] = [list(sorted_pages) for _ in range(self.total_columns)]
        self._tail_pages_created_since_merge = 0

    def _on_new_tail_page(self, column):
        if column != RID_COLUMN:
            return
        self._tail_pages_created_since_merge += 1
        if self._tail_pages_created_since_merge >= self.merge_tail_page_threshold:
            if self._merge_thread is None:
                self._merge_stop.clear()
                self._merge_thread = threading.Thread(target=self._merge_worker, daemon=True)
                self._merge_thread.start()
            self._merge_request.set()

    def _get_or_allocate_tail_page(self, range_index, column):
        self._ensure_tail_range(range_index)
        pages = self.tail_range_pages[range_index][column]
        if len(pages) == 0:
            page_index = len(self.tail_pages[column])
            self.tail_pages[column].append(None)
            pages.append(page_index)
            self.current_tail_page_index[column] = page_index
            self._on_new_tail_page(column)
            return page_index
        page_index = pages[-1]
        if self._page_has_capacity(True, column, page_index):
            self.current_tail_page_index[column] = page_index
            return page_index
        page_index = len(self.tail_pages[column])
        self.tail_pages[column].append(None)
        pages.append(page_index)
        self.current_tail_page_index[column] = page_index
        self._on_new_tail_page(column)
        return page_index

    def _read_record_from_directory(self, direction, is_tail):
        record = []
        for i in range(len(direction)):
            col_index = direction[i]
            if col_index[0] == 'N' or col_index[4] is None:
                value = None
            else:
                value = self._read_cell(is_tail, i, col_index[3], col_index[4])
            record.append(value)
        return record

    def _materialize_latest_from_snapshot(self, base_direction, snapshot_tail_rid):
        base_record = self._read_record_from_directory(base_direction, is_tail=False)
        if snapshot_tail_rid is None or not self.is_rid_tail_helper(snapshot_tail_rid):
            return base_record
        tail_record = self.read_record(snapshot_tail_rid)
        if tail_record is None:
            return base_record
        latest = list(tail_record)
        for i in range(len(latest)):
            if latest[i] is None:
                latest[i] = base_record[i]
        return latest

    def _reclaim_old_base_pages(self, old_pages_by_col):
        if self.bufferpool is None or self.disk_manager is None:
            return
        for col, pages in old_pages_by_col.items():
            for page_index in pages:
                self.bufferpool.discard_page(self.name, False, col, page_index, flush=False)
                self.disk_manager.delete_page(self.name, False, col, page_index)
        
     # We define the Rid for base page is positive, rid for tail page is negative
     # rid 0 means null or have been deleted
    def insert_base_record(self, columns): 
        base_rid = self.next_base_rid
        self.generate_rid(is_tail=False)
        indirection = None
        timestamp = int(time.time() * 1000)
        schema_encoding = 0
        metadata_columns = (indirection, base_rid, timestamp, schema_encoding)
        directory = []
        for i in range(self.total_columns):
            page_index = self.current_base_page_index[i]
            if not self._page_has_capacity(False, i, page_index):
                self.allocate_new_page(column_index=i, is_tail=False)
                page_index = self.current_base_page_index[i]
            # Identidy if it is metadata or data in column
            value = metadata_columns[i] if i < 4 else columns[i - 4]
            offset = self._append_cell(False, i, page_index, value)
            if offset is None:
                return None
            range_index = self._base_range_from_page_index(page_index)
            mark = 'B'
            if i == INDIRECTION_COLUMN and metadata_columns[i] is None:
                mark = 'N'
            # Page B_x_y_z, offset is the z-th Base page for column x in page range y
            directory.append((mark, i, range_index, page_index, offset))
        self.page_directory[base_rid] = directory
        self.base_rids.add(base_rid)
        self._sorted_base_rids_cache = None
        return base_rid
    
    """
    Helper function to write a new tail record if a value is first updated
    """
    def append_tail_record_first_time(self, base_rid, previous_rid, column):
        cur_tail_rid = self.next_tail_rid
        self.generate_rid(is_tail = True)
        base_range_index = self._get_base_range_for_rid(base_rid)
        indirection = previous_rid
        timestamp = int(time.time() * 1000)
        schema_encoding = (1 << self.num_columns) - 1
        base_column = self.read_record(base_rid)[4:]
        # Persist a full pre-update snapshot so old versions remain correct
        # even after merged base pages are refreshed in background.
        write_column = list(base_column)
        metadata_columns = (indirection, cur_tail_rid, timestamp, schema_encoding)
        directory = []
        for i in range(self.total_columns):
            page_index = self._get_or_allocate_tail_page(base_range_index, i)
            # Identidy if it is metadata or data in column
            if i < 4:
                offset = self._append_cell(True, i, page_index, metadata_columns[i])
                if offset is None:
                    return None
                directory.append(('T', i, base_range_index, page_index, offset))
            else:
                if write_column[i - 4] != None:
                    offset = self._append_cell(True, i, page_index, write_column[i - 4])
                    if offset is None:
                        return None
                    directory.append(('T', i, base_range_index, page_index, offset))
                else:
                    offset = self._append_cell(True, i, page_index, None)
                    if offset is None:
                        return None
                    directory.append(('N', i, base_range_index, page_index, offset))
        self.page_directory[cur_tail_rid] = directory
        self.star_tail_record.add(cur_tail_rid)
        return cur_tail_rid

    # We define the Rid for base page is positive, rid for tail page is negative
    # Cumulative Update
    def append_tail_record(self, columns, base_rid):
        columns = list(columns)
        tail_rid = self.next_tail_rid
        self.generate_rid(is_tail=True)
        base_range_index = self._get_base_range_for_rid(base_rid)

        base_record = self.read_record(base_rid)
        if base_record is None:
            return None
        previous_rid = base_rid
        if (base_record[0] is not None):
            latest_record = self.read_record(base_record[0])
            if latest_record is not None:
                previous_rid = latest_record[RID_COLUMN]

        timestamp = int(time.time() * 1000)
        schema_encoding = 0

        # Identify the Update Column and write the SE
        previous_record = self.read_record(previous_rid)
        if previous_record is None:
            return None
        previous_Schema_Encoding = previous_record[SCHEMA_ENCODING_COLUMN]
        if previous_Schema_Encoding is None:
            previous_Schema_Encoding = 0

        updated_column = [None] * self.num_columns
        first_time = previous_rid == base_rid
        for i in range(len(columns)):
            if columns[i] is not None:
                bit_left_move = self.num_columns - 1 - i
                schema_encoding |= (1<<bit_left_move)
            else:
                # Keep every tail as a full cumulative row.
                columns[i] = previous_record[i + 4]
        if first_time:
            previous_rid = self.append_tail_record_first_time(base_rid=base_rid, previous_rid=previous_rid, column=updated_column)
            if previous_rid is None:
                return None

        indirection = previous_rid
        # update the schema encoding with the previous schema encoding
        schema_encoding |= previous_Schema_Encoding
                
        metadata_columns = (indirection, tail_rid, timestamp, schema_encoding)
        directory = []

        for i in range(self.total_columns):
            page_index = self._get_or_allocate_tail_page(base_range_index, i)
            # Identidy if it is metadata or data in column
            if i < 4:
                offset = self._append_cell(True, i, page_index, metadata_columns[i])
                if offset is None:
                    return None
                if metadata_columns[i] is not None:
                    directory.append(('T', i, base_range_index, page_index, offset))
                else:
                    directory.append(('N', i, base_range_index, page_index, offset))
            else: 
                offset = self._append_cell(True, i, page_index, columns[i - 4])
                if offset is None:
                    return None
                if columns[i - 4] is not None:
                    directory.append(('T', i, base_range_index, page_index, offset))
                else: 
                    directory.append(('N', i, base_range_index, page_index, offset))
        self.page_directory[tail_rid] = directory

        # Update the indirection and SE of base record after the update
        self.update_indirection(base_rid, tail_rid)
        self.update_SE(base_rid, schema_encoding)
        return tail_rid

    def delete_record(self, rid):
        if rid is None or rid not in self.page_directory:
            return False
        direction = self.page_directory[rid]
        col_index= direction[RID_COLUMN]
        if self.is_rid_tail_helper(rid):
            status = self._update_cell(True, RID_COLUMN, col_index[3], col_index[4], 0)
        else:
            status = self._update_cell(False, RID_COLUMN, col_index[3], col_index[4], 0)
        value = self.page_directory.pop(rid, None)
        if rid > 0:
            self.base_rids.discard(rid)
            self.tps.pop(rid, None)
            self._sorted_base_rids_cache = None
        return status & (value != None)
    

    def read_record(self, rid):
        if rid is None:
            return None
        direction = self.page_directory.get(rid)
        if direction is None:
            return None
        record = []
        if self.is_rid_tail_helper(rid):
            for i in range(len(direction)):
                col_index = direction[i]
                if col_index[0] == 'N':
                    value = None
                elif col_index[4] is None:
                    value = None
                else:
                    value = self._read_cell(True, i, col_index[3], col_index[4])
                record.append(value)
        else:
             for i in range(len(direction)):
                col_index = direction[i]
                if col_index[0] == 'N':
                    value = None
                elif col_index[4] is None:
                    value = None
                else:
                    value = self._read_cell(False, i, col_index[3], col_index[4])
                record.append(value)
        return record
    
    def read_latest_record(self, base_rid):
        record = self.read_record(base_rid)
        if record is None:
            return None
        latest_record = record
        latest_tail_rid = record[0]
        if latest_tail_rid is not None and self.is_rid_tail_helper(latest_tail_rid):
            tps = self.tps.get(base_rid)
            # Tail RIDs decrease monotonically: values < TPS are newer than merged state.
            need_tail_lookup = (tps is None) or (latest_tail_rid < tps)
            if need_tail_lookup:
                tail = self.read_record(latest_tail_rid)
                if tail is not None:
                    latest_record = tail
        for i in range(len(latest_record)):
            if latest_record[i] is None:
                latest_record[i] = record[i]
        return latest_record
    
    """
    Modified function for enabling tracing the version
    """
    def read_latest_record_modified(self, base_rid, relative_version):
        record = self.read_record(base_rid)
        if record is None:
            return None
        latest_rid = record[0]
        if latest_rid is None or latest_rid == 0:
            return record

        if relative_version >= 0:
            tps = self.tps.get(base_rid)
            if tps is not None and self.is_rid_tail_helper(latest_rid) and latest_rid >= tps:
                return record

        latest_record = self.read_record(latest_rid)
        if latest_record is None:
            return record
        if relative_version >= 0:
            for i in range(len(latest_record)):
                if latest_record[i] is None:
                    latest_record[i] = record[i]
            return latest_record

        steps = -relative_version
        cur_rid = latest_rid
        cur_record = latest_record
        while steps > 0:
            prev_rid = cur_record[0]
            if prev_rid is None or prev_rid == 0:
                break
            # Keep the earliest snapshot as the floor for very old versions.
            if cur_rid in self.star_tail_record and not self.is_rid_tail_helper(prev_rid):
                break

            prev_record = self.read_record(prev_rid)
            if prev_record is None:
                break
            cur_rid = prev_rid
            cur_record = prev_record
            steps -= 1

        for i in range(len(cur_record)):
            if cur_record[i] is None:
                cur_record[i] = record[i]
        return cur_record

    def update_indirection(self, base_rid, new_tail_rid):
        if base_rid not in self.page_directory:
            return
        direction = self.page_directory[base_rid]
        indirection_index = direction[INDIRECTION_COLUMN]
        self._update_cell(False, INDIRECTION_COLUMN, indirection_index[3], indirection_index[4], new_tail_rid)
        if indirection_index[0] == 'N':
            self.page_directory[base_rid][INDIRECTION_COLUMN] = ('B', indirection_index[1], indirection_index[2], indirection_index[3], indirection_index[4])
    
    """
    function to update schema encoding of base pages Record
    """
    def update_SE(self, rid, updated_SE):
        if rid not in self.page_directory:
            return
        direction = self.page_directory[rid]
        SE_index = direction[SCHEMA_ENCODING_COLUMN]
        self._update_cell(False, SCHEMA_ENCODING_COLUMN, SE_index[3], SE_index[4], updated_SE)


    def allocate_new_page(self, column_index, is_tail):
        if (is_tail):
            self.tail_pages[column_index].append(None)
            self.current_tail_page_index[column_index] += 1
        else:
            self.base_pages[column_index].append(None)
            self.current_base_page_index[column_index] += 1

    def generate_rid(self, is_tail):
        if is_tail:
            self.next_tail_rid = self.next_tail_rid - 1
        else:
            self.next_base_rid = self.next_base_rid + 1

    def is_rid_tail_helper(self, rid):
        if rid is None:
            return False
        return rid < 0

    def __merge(self):
        """
        1. All non-metadata columns contain the latest values.
        2. While the merge is in progress, two copies of the base pages will be kept in memory
        3. Update page directory (Create a new one or Lock it).
        4. Free the space occupied by the old base page and the old page directory.
        """
        with self.latch:
            if len(self.base_rids) == 0:
                self._tail_pages_created_since_merge = 0
                return
            range_snapshots = {}
            for rid in list(self.base_rids):
                direction = self.page_directory.get(rid)
                if direction is None:
                    continue
                range_index = direction[RID_COLUMN][2]
                indirection_loc = direction[INDIRECTION_COLUMN]
                if indirection_loc[0] == 'N' or indirection_loc[4] is None:
                    snapshot_tail_rid = None
                else:
                    snapshot_tail_rid = self._read_cell(False, INDIRECTION_COLUMN, indirection_loc[3], indirection_loc[4])
                range_snapshots.setdefault(range_index, []).append((rid, list(direction), snapshot_tail_rid))

        for range_index, entries in range_snapshots.items():
            if len(entries) == 0:
                continue
            entries.sort(key=lambda x: x[0])

            old_pages_by_col = {col + 4: set() for col in range(self.num_columns)}
            current_write_page = {col + 4: None for col in range(self.num_columns)}
            merged_locations = {}

            for rid, old_dir, snapshot_tail_rid in entries:
                latest = self._materialize_latest_from_snapshot(old_dir, snapshot_tail_rid)
                if latest is None:
                    continue

                row_locations = {}
                row_ok = True
                for col in range(self.num_columns):
                    page_col = col + 4
                    old_pages_by_col[page_col].add(old_dir[page_col][3])

                    target_page = current_write_page[page_col]
                    if target_page is None or not self._page_has_capacity(False, page_col, target_page):
                        target_page = len(self.base_pages[page_col])
                        self.base_pages[page_col].append(None)
                        current_write_page[page_col] = target_page

                    offset = self._append_cell(False, page_col, target_page, latest[page_col])
                    if offset is None:
                        target_page = len(self.base_pages[page_col])
                        self.base_pages[page_col].append(None)
                        current_write_page[page_col] = target_page
                        offset = self._append_cell(False, page_col, target_page, latest[page_col])

                    if offset is None:
                        row_ok = False
                        break
                    row_locations[page_col] = ('B', page_col, range_index, target_page, offset)

                if row_ok and len(row_locations) == self.num_columns:
                    merged_locations[rid] = row_locations

            with self.latch:
                self._pending_merge_jobs.append(
                    (range_index, entries, merged_locations, old_pages_by_col)
                )

        with self.latch:
            self._tail_pages_created_since_merge = 0

    def _merge_worker(self):
        while not self._merge_stop.is_set():
            has_request = self._merge_request.wait(timeout=0.2)
            if self._merge_stop.is_set():
                break
            if not has_request:
                continue
            self._merge_request.clear()
            self.__merge()

    def get_base_rids(self):
        # Live base records only.
        if self._sorted_base_rids_cache is None:
            self._sorted_base_rids_cache = sorted(self.base_rids)
        return self._sorted_base_rids_cache

    def apply_pending_merges_foreground(self):
        reclaim_batches = []
        with self.latch:
            if len(self._pending_merge_jobs) == 0:
                return 0
            jobs = self._pending_merge_jobs
            self._pending_merge_jobs = []
            applied = 0
            for range_index, entries, merged_locations, old_pages_by_col in jobs:
                old_pd = self.page_directory
                new_pd = dict(old_pd)
                merged_rids = []
                for rid, _old_dir, snapshot_tail_rid in entries:
                    if rid not in merged_locations:
                        continue
                    cur_dir = old_pd.get(rid)
                    if cur_dir is None:
                        continue
                    if cur_dir[RID_COLUMN][2] != range_index:
                        continue
                    new_dir = list(cur_dir)
                    row_locations = merged_locations[rid]
                    for col in range(self.num_columns):
                        page_col = col + 4
                        new_dir[page_col] = row_locations[page_col]
                    new_pd[rid] = new_dir
                    """
                    Tail-page sequence number (TPS):
                    Keep track of how many tail records from tail pages have been applied to their corresponding 
                    base pages after a completion of a merge.
                    """
                    self.tps[rid] = snapshot_tail_rid if self.is_rid_tail_helper(snapshot_tail_rid) else None
                    merged_rids.append(rid)

                self.page_directory = new_pd
                if len(merged_rids) == len(entries) and len(merged_rids) > 0:
                    reclaim_batches.append(old_pages_by_col)
                applied += len(merged_rids)

        for old_pages_by_col in reclaim_batches:
            self._reclaim_old_base_pages(old_pages_by_col)
        return applied

    def save(self, disk_manager):
        self.shutdown()
        self.apply_pending_merges_foreground()
        if self.bufferpool is not None:
            self.bufferpool.flush_all(self.name)

        table_path = os.path.join(disk_manager.path, self.name)
        if not os.path.exists(table_path):
            os.makedirs(table_path, exist_ok=True)

        meta_path = os.path.join(disk_manager.path, self.name, "metadata.txt")
        f = open(meta_path, "w")
        f.write(str(self.num_columns) + "\n")
        f.write(str(self.key) + "\n")
        f.write(str(self.next_base_rid) + "\n")
        f.write(str(self.next_tail_rid) + "\n")
        f.close()

        pd_path = os.path.join(disk_manager.path, self.name, "page_directory.txt")
        f = open(pd_path, "w")
        for rid in sorted(self.page_directory.keys()):
            direction = self.page_directory[rid]
            parts = []
            for entry in direction:
                mark, col, range_index, page_index, offset = entry
                off = -1 if offset is None else int(offset)
                parts.append(
                    f"{mark},{int(col)},{int(range_index)},{int(page_index)},{off}"
                )
            f.write(f"{int(rid)}|{';'.join(parts)}\n")
        f.close()

        tps_path = os.path.join(disk_manager.path, self.name, "tps.txt")
        f = open(tps_path, "w")
        for rid in sorted(self.tps.keys()):
            value = self.tps[rid]
            value_str = "N" if value is None else str(int(value))
            f.write(f"{int(rid)}|{value_str}\n")
        f.close()

        star_path = os.path.join(disk_manager.path, self.name, "star_tail.txt")
        f = open(star_path, "w")
        for rid in sorted(self.star_tail_record):
            f.write(str(int(rid)) + "\n")
        f.close()

    def shutdown(self):
        if self._merge_thread is None:
            return
        self._merge_request.set()
        self._merge_stop.set()
        self._merge_thread.join(timeout=1.0)
        self._merge_thread = None
    
 
