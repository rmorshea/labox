# Storages

A few built-in [storage](../../concepts/storages.md) implementations are available in
Labox.

## Database Storage

The database storage is a built-in storage that saves content under the JSON (JSONB for
PostgreSQL) `storage_config` column of a
[`ContentRecord`](../../concepts/database.md#content-records). This storage is best used
when the content is small and needs to be leveraged when querying the database.

Because the content is held in a JSON (or JSONB) column, this storage is limited to
saving JSON data. To enforce this the storage checks that the `content_type` is set to
`application/json` or the same with an extension (e.g. `application/json+x-labox`). This
storage also enforces a maximum size for the content it saves since direct storage in
the database is not recommended for large artifacts. By default the max size is 100kb
with a warning at 10kb. You can configure this maximum size by passing a `warn_size`
and/or `error_size` to the
[`DatabaseStorage`][labox.builtin.storages.database.DatabaseStorage] constructor.

```python
from labox.builtin import DatabaseStorage, database_storage

db_storage = DatabaseStorage(
    warn_size=100 * 1024,  # 100kb
    error_size=1000 * 1024,  # 1mb
)
```

## File System

A file based storage is available through the
[`FileStorage`][labox.builtin.storages.file.FileStorage] class. This storage saves
content to the file system under a directory. The default instance of this storage
(`file_storage`) uses a temporary directory that is deleted when the process exits
making it suitable for testing purposes.

```python
from labox.builtin import FileStorage, file_storage

my_file_storage = FileStorage("/path/to/storage")
```

## Memory Storage

A memory based storage is available through the
[`MemoryStorage`][labox.builtin.storages.memory.MemoryStorage] class. This storage saves
content in memory and is best suited for testing purposes.

```python
from labox.builtin import memory_storage
```
