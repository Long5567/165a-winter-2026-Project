"""
This functions to build Index manager for key and optional secondary columns.
"""

from bisect import bisect_left, bisect_right, insort


class Index:

    def __init__(self, table):
        self.indices = [None] * table.num_columns
        self.table = table
        # Primary-key index: key value -> base RID
        self.indices[self.table.key] = {}
        self.sorted_keys = []

    def insert_key(self, key, rid):
        key_index = self.indices[self.table.key]
        if key in key_index:
            return False
        key_index[key] = rid
        insort(self.sorted_keys, key)
        return True

    def _insert_secondary(self, column, value, rid):
        if self.indices[column] is None:
            return
        bucket = self.indices[column].setdefault(value, set())
        bucket.add(rid)

    def _remove_secondary(self, column, value, rid):
        if self.indices[column] is None:
            return
        bucket = self.indices[column].get(value)
        if bucket is None:
            return
        bucket.discard(rid)
        if len(bucket) == 0:
            self.indices[column].pop(value, None)

    def add_record(self, rid, columns):
        """
        Add a base record to all active secondary indexes.
        columns is the logical row: [c0, c1, ...].
        """
        for col in range(self.table.num_columns):
            if col == self.table.key:
                continue
            if self.indices[col] is None:
                continue
            self._insert_secondary(col, columns[col], rid)

    def remove_record(self, rid, columns):
        """
        To remove a base record from all active secondary indexes.
        """
        for col in range(self.table.num_columns):
            if col == self.table.key:
                continue
            if self.indices[col] is None:
                continue
            self._remove_secondary(col, columns[col], rid)

    def update_record(self, rid, old_columns, new_columns):
        """
        To update secondary indexes after a row update.
        """
        for col in range(self.table.num_columns):
            if col == self.table.key:
                continue
            if self.indices[col] is None:
                continue
            old_val = old_columns[col]
            new_val = new_columns[col]
            if old_val == new_val:
                continue
            self._remove_secondary(col, old_val, rid)
            self._insert_secondary(col, new_val, rid)

    def locate(self, column, value):
        if column < 0 or column >= self.table.num_columns:
            return None
        if self.indices[column] is None:
            return None
        if column == self.table.key:
            return self.indices[column].get(value)
        rid_set = self.indices[column].get(value)
        if rid_set is None:
            return ()
        # Return the internal set directly to avoid per-query list copies.
        return rid_set
    # To locate matching records:= column == value
    def locate_range(self, begin, end, column):
        if column != self.table.key:
            return []
        left = bisect_left(self.sorted_keys, begin)
        right = bisect_right(self.sorted_keys, end)
        keys_in_range = self.sorted_keys[left:right]
        key_index = self.indices[self.table.key]
        return [key_index.get(pk) for pk in keys_in_range]

    def delete_index(self, key):
        # current implementation with delete primary-key entry by key value
        key_index = self.indices[self.table.key]
        if key not in key_index:
            return False
        key_index.pop(key, None)
        i = bisect_left(self.sorted_keys, key)
        if i < len(self.sorted_keys) and self.sorted_keys[i] == key:
            self.sorted_keys.pop(i)
        return True

    def create_index(self, column_number):
        if column_number < 0 or column_number >= self.table.num_columns:
            return False
        if column_number == self.table.key:
            return True
        if self.indices[column_number] is not None:
            return True
        self.indices[column_number] = {}
        # Build from current latest value of every existing base record
        for rid in self.table.get_base_rids():
            latest = self.table.read_latest_record(rid)
            if latest is None:
                continue
            value = latest[4 + column_number]
            self._insert_secondary(column_number, value, rid)
        return True

    def drop_index(self, column_number):
        if column_number < 0 or column_number >= self.table.num_columns:
            return False
        if column_number == self.table.key:
            # Primary-key index cannot be dropped
            return False
        self.indices[column_number] = None
        return True
