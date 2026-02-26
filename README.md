# 165a-winter-2026

## Milestone 1: L-Store
This repository contains our L-Store storage programe implementation for Milestone 1 task. \
The programme would store records in columnar format, and supports basic SQL-similar operations.

### Properties:

- Supports Single-threaded and in-memory storage
- Provides L-Store-like storage with base records and appendable tail records
- Utilizes Primary-key index supporting point lookups, and inclusive range scan/search.

### Run
```bash
pip install -r requirements.txt
python __main__.py
python m1_tester.py
python m1_tester_new.py
```
### Features:

##### Query Method("lstore/query.py")

- `insert(*columns) -> bool`  
  To insert one base record. The key is `columns[key_index]`. Would return `False`if Duplicated keys appeared .

- `select(search_key, search_key_index, projected_columns_index) -> list[Record] | bool`  
  To look-up primary key. Would return `False` if `search_key_index != key_index`.

- `update(primary_key, *columns) -> bool`  
  To append 1 tail record and set the base indirection to the newest tail Record ID (RID).  
  Function is cumulative: `None` column would carry prior values

- `delete(primary_key) -> bool`  
  For logical delete of the base record for the key: the function removes the key from the index and removes the base RID from `page_directory`.  

- `sum(start_range, end_range, aggregate_column_index) -> int | bool`  
  To calculate inclusive key-range sum by primary-key range index.

##### Storage model

- Columnar pages: all column are stored in fixed-size `Page` objects supported by a 4096-byte `bytearray`. 
all entry are stored as signed 64-bit integers (occupies 8 bytes), so one page could store up to 512 values.
- Base record column (4 columns) includes: indirection, RID, timestamp, schema encoding.
- Base record use positive RID that would monotonically increase while Tail record use negative RID that monotonically decrease; Tail record only open to append.
- Indirection chain: The base record’s indirection points to the most recent tail RID, and tail record stores an indirection pointer to the previous RID in the Indirection chain
- Page directory: A `page_directory` is used to mapping all RIDs to their physical coordinates per column: (page type, column, range, page index, offset).
