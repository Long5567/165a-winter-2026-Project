from collections import OrderedDict
from dataclasses import dataclass
from lstore.config import PAGE_SIZE


@dataclass
class BufferFrame:
    data: bytearray
    num_records: int
    dirty: bool = False
    pin_count: int = 0


class BufferPool:
    """
    bufferpool manager with fixed bufferpool size capacity, LRU eviction, dirty-page tracking, and pin/unpin function.
    """

    def __init__(self, disk_manager, capacity):
        self.disk_manager = disk_manager
        self.capacity = capacity
        self.frames = {}  # key: key value: BufferFrame
        # LRU order: oldest at front and newest at end
        self.lru = OrderedDict()

    @staticmethod
    def make_key(table_name, is_tail, column, page_index):
        return (table_name, bool(is_tail), int(column), int(page_index))

    def _touch(self, key):
        if key in self.lru:
            self.lru.move_to_end(key)
        else:
            self.lru[key] = None

    def _load_from_disk(self, key):
        table_name, is_tail, column, page_index = key
        raw = self.disk_manager.read_page(table_name, is_tail, column, page_index)
        if raw is None:
            data = bytearray(PAGE_SIZE)
            count = 0
        else:
            # concurrent IO can expose short reads
            # make every frame at fixed PAGE_SIZE bytes 
            data = bytearray(PAGE_SIZE)
            n = min(len(raw), PAGE_SIZE)
            if n > 0:
                data[:n] = raw[:n]
            count = self.disk_manager.read_page_count(table_name, is_tail, column, page_index)
            if count < 0:
                count = 0
            max_records = PAGE_SIZE // 8
            if count > max_records:
                count = max_records
        return BufferFrame(data=data, num_records=count)
    
    """
    If evicted page has been updated (dirty), we need to write it back to Disk.
    """
    def _evict_if_needed(self):
        if len(self.frames) < self.capacity:
            return True

        # Evict oldest unpinned page
        for key in list(self.lru.keys()):
            frame = self.frames.get(key)
            if frame is None:
                self.lru.pop(key, None)
                continue
            if frame.pin_count == 0:
                self.flush_page(key)
                self.frames.pop(key, None)
                self.lru.pop(key, None)
                return True
        return False

    def fetch_page(self, table_name, is_tail, column, page_index, pin=True):
        key = self.make_key(table_name, is_tail, column, page_index)
        frame = self.frames.get(key)
        if frame is None:
            if not self._evict_if_needed():
                return None
            frame = self._load_from_disk(key)
            self.frames[key] = frame
        if pin:
            frame.pin_count += 1
        self._touch(key)
        return frame

    def mark_dirty(self, table_name, is_tail, column, page_index):
        key = self.make_key(table_name, is_tail, column, page_index)
        frame = self.frames.get(key)
        if frame is None:
            return False
        frame.dirty = True
        self._touch(key)
        return True
    
    """
    Pinning/Unpinning Pages
    • Anytime a page in the bufferpool is accessed, the page will be pinned.
    • Once the transaction no longer needs the page, the page will be unpinned.
    • Pin value records the number of transactions accessing the page.
    • The page cannot be replaced if the pin value is not zero.
    """

    def pin_page(self, table_name, is_tail, column, page_index):
        key = self.make_key(table_name, is_tail, column, page_index)
        frame = self.frames.get(key)
        if frame is None:
            return False
        frame.pin_count += 1
        self._touch(key)
        return True

    def unpin_page(self, table_name, is_tail, column, page_index):
        key = self.make_key(table_name, is_tail, column, page_index)
        frame = self.frames.get(key)
        if frame is None:
            return False
        if frame.pin_count > 0:
            frame.pin_count -= 1
        self._touch(key)
        return True

    def flush_page(self, key):
        frame = self.frames.get(key)
        if frame is None:
            return False
        if not frame.dirty:
            return True
        table_name, is_tail, column, page_index = key
        self.disk_manager.write_page(table_name, is_tail, column, page_index, frame.data, frame.num_records)
        frame.dirty = False
        return True

    def flush_all(self, table_name=None):
        keys = list(self.frames.keys())
        for key in keys:
            if table_name is not None and key[0] != table_name:
                continue
            self.flush_page(key)
        return True

    def discard_page(self, table_name, is_tail, column, page_index, flush=False):
        key = self.make_key(table_name, is_tail, column, page_index)
        frame = self.frames.get(key)
        if frame is None:
            self.lru.pop(key, None)
            return True
        if flush:
            self.flush_page(key)
        self.frames.pop(key, None)
        self.lru.pop(key, None)
        return True

    def size(self):
        return len(self.frames)
