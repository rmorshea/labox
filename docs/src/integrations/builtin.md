# Builtin

## Storables

Labox provides a handful of built-in [storables](../concepts/storables.md). Most notably
for dataclasses.

### Dataclasses

Labox provides a base
[`StorableDataclass`][labox.builtin.storables.dataclasses.StorableDataclass] class that
can be added to a dataclass to make it [storable](../usage/index.md#saving-storables).
After inheriting from this base class each field metadata can be used to specify an
explicit serializer and storage for that field.

```python
from dataclasses import dataclass
from dataclasses import field

from labox.builtin import CsvSerializer
from labox.builtin import StorableDataclass


@dataclass
class MyDataClass(StorableDataclass):
    data: list[list[int]] = field(metadata={"serializer": CsvSerializer})
```

Ultimately a dataclass is stored under one or more
[`ContentRecord`s](../concepts/database.md#content-records). The majority of the
dataclass is stored in a "body" record, which is nominally a JSON serializable object.
The serializer and storage for this body record can be customized by overriding the
[`storable_body_serializer`][labox.builtin.storables.dataclasses.StorableDataclass.storable_body_serializer]
and/or
[`storable_body_storage`][labox.builtin.storables.dataclasses.StorableDataclass.storable_body_storage]
methods in the dataclass itself.

Fields within the dataclass are stored within this same body record unless they have a
separate `storage` or [stream serializer](../concepts/serializers.md#stream-serializers)
specified in their metadata.

### Simple Values

If all you need to do is store a single value you can do so using the
[`StorableValue`][labox.builtin.storables.simple.StorableValue] class. In addition to
the value you want to save you can manually specify its
[serializer](../concepts/serializers.md) and [storage](../concepts/storages.md):

```python
from labox.builtin import JsonSerializer
from labox.builtin import StorableValue

storable = StorableValue(value="Hello, World!", serializer=JsonSerializer)
```

### Simple Streams

Similarly, if you want to store a stream of values, you can use the
[`StorableStream`][labox.builtin.storables.simple.StorableStream] class. It works
similarly to [`StorableValue`][labox.builtin.storables.simple.StorableValue] but is
designed for storing an asynchronous stream of values. You can also specify a serializer
and storage:

```python
from labox.builtin import JsonStreamSerializer
from labox.builtin import StorableStream

storable = StorableStream(value="Hello, World!", serializer=JsonStreamSerializer)
```

## Serializers

Labox provides built-in [serializers](../concepts/serializers.md) for various stdlib
data types.

### JSON

Both a [`JsonSerializer`][labox.builtin.serializers.JsonSerializer] and
[`JsonStreamSerializer`][labox.builtin.serializers.JsonStreamSerializer] implementations
are available. Either can be configured with an optional
[`JSONEncoder`][json.JSONEncoder] and/or [`JSONDecoder`][json.JSONDecoder].

```python
import json

from labox.builtin import JsonSerializer
from labox.builtin import json_serializer
from labox.builtin import json_stream_serializer

custom_json_serializer = JsonSerializer(
    encoder=json.JSONEncoder(indent=2),
    decoder=json.JSONDecoder(object_hook=lambda d: d),
)
```

### CSV

A [`CsvSerializer`][labox.builtin.serializers.CsvSerializer] implementation is
available. It can be configured with
[`CsvOptions`][labox.builtin.serializers.CsvOptions] that are similat to those passed to
[`csv.writer`][csv.writer] and [`csv.reader`][csv.reader]. Unlike those though, you
cannot pass a [`csv.Dialect`][csv.Dialect] directly. Instead, you may pass a dialect
name as a string. For custom dialects, you can first use
[`csv.register_dialect`][csv.register_dialect] to add it under a name you choose, and
then pass that name to the serializer.

```python
from labox.builtin import CsvSerializer
from labox.builtin import csv_serializer

unix_csv_serializer = CsvSerializer(dialect="unix")
```

### Datetime

Labox provides a basic
[`Iso8601Serializer`][labox.builtin.serializers.Iso8601Serializer] for serializing
datetime objects to ISO 8601 strings.

```python
from labox.builtin import iso8601_serializer
```

## Storages

A few built-in [storage](../concepts/storages.md) implementations are available in
Labox.

### Database Storage

The database storage is a built-in storage that saves content under the JSON (JSONB for
PostgreSQL) `storage_config` column of a
[`ContentRecord`](../concepts/database.md#content-records). This storage is best used
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
from labox.builtin import DatabaseStorage
from labox.builtin import database_storage

db_storage = DatabaseStorage(
    warn_size=100 * 1024,  # 100kb
    error_size=1000 * 1024,  # 1mb
)
```

### File System

A file based storage is available through the
[`FileStorage`][labox.builtin.storages.file.FileStorage] class. This storage saves
content to the file system under a directory. The default instance of this storage
(`file_storage`) uses a temporary directory that is deleted when the process exits
making it suitable for testing purposes.

```python
from labox.builtin import FileStorage
from labox.builtin import file_storage

my_file_storage = FileStorage("/path/to/storage")
```

### Memory Storage

A memory based storage is available through the
[`MemoryStorage`][labox.builtin.storages.memory.MemoryStorage] class. This storage saves
content in memory and is best suited for testing purposes.

```python
from labox.builtin import memory_storage
```
