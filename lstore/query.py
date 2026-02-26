from lstore.table import Record


class Query:
    def __init__(self, table):
        self.table = table

    def _project_record(self, rid, key, record, projected_columns_index):
        data = record[4:]
        full = [None] * self.table.num_columns
        for i in range(self.table.num_columns):
            if projected_columns_index[i] == 1:
                full[i] = data[i]
        return Record(rid=rid, key=key, columns=full)

    def delete(self, primary_key):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False

            latest = self.table.read_latest_record(rid)
            if latest is None:
                return False
             # Keep secondary indexes consistent
            self.table.index.remove_record(rid, latest[4:])

            success = self.table.delete_record(rid)
            if success:
                self.table.index.delete_index(primary_key)
            return success

    def insert(self, *columns):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            if len(columns) != self.table.num_columns:
                return False
            if any(col is None for col in columns):
                return False

            key = columns[self.table.key]
            if self.table.index.locate(self.table.key, key) is not None:
                return False

            base_rid = self.table.insert_base_record(columns)
            if base_rid is None:
                return False
            
            # Maintaining primary key mapping + secondary index payloads
            self.table.index.insert_key(key, base_rid)
            self.table.index.add_record(base_rid, list(columns))
            return True

    def select(self, search_key, search_key_index, projected_columns_index):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            if search_key_index < 0 or search_key_index >= self.table.num_columns:
                return False

            result = []

            if search_key_index == self.table.key:
                rid = self.table.index.locate(self.table.key, search_key)
                if rid is None:
                    return []
                record = self.table.read_latest_record(rid)
                if record is None:
                    return []
                result.append(self._project_record(rid, search_key, record, projected_columns_index))
                return result
            
            #  if Non-key column
            #  see secondary index locate() first
            #  if not available, fall back to full scan base RIDs
            rid_list = self.table.index.locate(search_key_index, search_key)
            if rid_list is None:
                rid_list = []
                for rid in self.table.get_base_rids():
                    record = self.table.read_latest_record(rid)
                    if record is None:
                        continue
                    if record[4 + search_key_index] == search_key:
                        rid_list.append(rid)

            for rid in rid_list:
                record = self.table.read_latest_record(rid)
                if record is None:
                    continue
                key = record[4 + self.table.key]
                result.append(self._project_record(rid, key, record, projected_columns_index))
            return result

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            if search_key_index < 0 or search_key_index >= self.table.num_columns:
                return False
            if search_key_index != self.table.key:
                return False

            rid = self.table.index.locate(self.table.key, search_key)
            if rid is None:
                return []
            record = self.table.read_latest_record_modified(rid, relative_version)
            if record is None:
                return []
            return [self._project_record(rid, search_key, record, projected_columns_index)]

    def update(self, primary_key, *columns):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False

            old_latest = self.table.read_latest_record(rid)
            if old_latest is None:
                return False

            key_column = self.table.key
            new_key = columns[key_column] if key_column < len(columns) else None
            if new_key is not None and new_key != primary_key:
                return False

            tail_rid = self.table.append_tail_record(columns, rid)
            if tail_rid is None:
                return False

            new_latest = self.table.read_latest_record(rid)
            if new_latest is not None:
                self.table.index.update_record(rid, old_latest[4:], new_latest[4:])

            return True

    def sum(self, start_range, end_range, aggregate_column_index):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
            if len(rid_list) == 0:
                return False
            total = 0
            for rid in rid_list:
                if rid is None:
                    continue
                cur_record = self.table.read_latest_record(rid)
                if cur_record is None:
                    continue
                total += cur_record[aggregate_column_index + 4]
            return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        with self.table.latch:
            # Apply merges in the foreground while holding the latch so
            # page_directory swaps don't race with reads during this query.
            self.table.apply_pending_merges_foreground()
            rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
            if len(rid_list) == 0:
                return False
            total = 0
            for rid in rid_list:
                # relative_version: 0 means latest, -1 means previous 
                cur_record = self.table.read_latest_record_modified(rid, relative_version)
                if cur_record is None:
                    continue
                total += cur_record[aggregate_column_index + 4]
            return total

    def increment(self, key, column):
        with self.table.latch:
            self.table.apply_pending_merges_foreground()
            selected = self.select(key, self.table.key, [1] * self.table.num_columns)
            if not selected:
                return False
            r = selected[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            return self.update(key, *updated_columns)
