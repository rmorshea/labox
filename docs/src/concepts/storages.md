# Storages

Laykery storages are used to persist data from a [serializer](./serializers.md). All
storage implementations must support persisting singular blobs of data as well as
streams of data when subclassing the [`Storage`][labox.core.storage.Storage] base class.

## Defining a Storage

To define a storage you need to implement the [`Storage`][labox.core.storage.Storage]
interface with the following:

- `name` - a string that uniquely and permanently identifies the storage.
- `write_data` - a method that saves a single blob of data to the storage.
- `read_data` - a method that reads a single blob of data from the storage.
- `write_data_stream` - a method that saves a stream of data to the storage.
- `read_data_stream` - a method that reads a stream of data from the storage.

The code snippets below show a storage that saves data to files. You can start by
implementing the `write_data` and `read_data` methods:

```python
from pathlib import Path

from labox import Digest
from labox import Storage
from labox import TagMap


class FileStorage(Storage):
    name = "temp-file-storage@v1"

    def __init__(self, prefix: Path, *, read_chunk_size: int = 1024):
        self._prefix = prefix
        self._read_chunk_size = read_chunk_size

    async def write_data(self, data: bytes, digest: Digest, tags: TagMap) -> str:
        path = self._prefix / digest["content_hash"]
        with path.open("wb") as f:
            f.write(data)
        return str(path)

    async def read_data(self, path: str) -> bytes:
        with Path(path).open("rb") as f:
            return f.read()
```

You can then implement the `write_data_stream` and `read_data_stream` methods to handle
streams of data. This is a bit trickier since the `conten_hash` of the serialized data
is not known until the stream has been fully read. Consequently, the data must be
written to a temporary file first, and then the final `content_hash` can be computed and
the file renamed to the final key.

```python
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from tempfile import NamedTemporaryFile


class FileStorage(Storage):
    ...

    async def write_data_stream(
        self,
        stream: AsyncIterator[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
    ) -> str:
        with NamedTemporaryFile(dir=self._root) as temp_file:
            async for chunk in stream:
                temp_file.write(chunk)
            temp_file.flush()
            temp_file.seek(0)
            key = get_digest()["content_hash"]
            Path(temp_file.name).rename(self._root / key)
        return key

    async def read_data_stream(self, key: str) -> AsyncGenerator[bytes]:
        with (self._root / key).open("rb") as f:
            while chunk := f.read(self._read_chunk_size):
                yield chunk
```

## Content Digest

The [`Digest`][labox.core.storage.Digest] for a piece of content passed to the `write_*`
methods of a storage provides extra information about the content (e.g. its size, type,
and hash). In the case of streamed data a function
([`GetStreamDigest`][labox.core.storage.GetStreamDigest]) to retrieve the digest is
passed as an argument instead. If the stream has not been fully consumed
`allow_incomplete=True` must be passed when retrieving the digest to avoid a
`ValueError`.

## Best Practices

When implementing a storage, the most important thing to keep in mind is that a storage
implementation must be able to read from any location is has written to. So, for
example, if one of the configuration options your storage accepts is a path prefix (as
in the example above), then this prefix must be included in the
[storage config](#storage-config) returned by the `write_data` and `write_data_stream`
methods. This way, when reading data, the storage can reconstruct the full path to the
data even if the prefix may have changed since the data was written.

A pattern used within Labox when implementing a storage is to allow users to configure
their storages with a "router" function that takes in the `Digest` and `tags` of the
data being saved and returns a dictionary with the storage-specific information needed
to locate the data later. In the case of the [`S3Storage`][labox.extra.aws.S3Storage],
the router function must return an [`S3Pointer`][labox.extra.aws.S3Pointer] dictionary
with the `bucket` and `key` where the data is stored. This forces the storage
implementation to be agnostic about where it's been configured to save data while still
allowing it to save data in a location that can be reconstructed later.

## Storage Tags

In the example above, the `write_data` and `write_data_stream` methods accept a`tags`
argument. This is a dictionary of tags that were provided when
[saving](../usage/index.md#adding-tags) the data.

## Storage Config

When a storage saves data via its `write_data` and `write_data_stream` methods,
information that is used to retrieve it later is returned. This information is called
"storage config" and is distinct from the data which is being stored remotely. In the
example above, the storage config is a string that forms part of a file path where the
data was put. More generally though, these methods may return anything which is JSON
serializable. You may customize how this data is serialized and deserialized by
replacing the default
[`Storage.serialize_config`][labox.core.storage.Storage.serialize_config] and
[`Storage.deserialize_config`][labox.core.storage.Storage.deserialize_config] methods.
Ultimately this `config` is saved within the
[`ContentRecord.storage_config`][labox.core.database.ContentRecord.storage_config]
column in the database.

## Storage Names

Storages are identified by a globally unique name associated with each storage class
within a [registry](./registry.md#adding-storages). Once a storage has been used to
saved data this name must never be changed and the implementation of the storage must
always remain backwards compatible. If you need to change the implementation of a
storage you should create a new one with a new name. In order to continue loading old
data you'll have to preserve and registry the old storage.
