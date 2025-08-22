# Google

!!! note

    Install with `pip install labox[google]`

## Cloud Storage

The [`BlobStorage`][labox.extra.google.BlobStorage] provides a
[storage](../concepts/storages.md) implementation backed by
[Google Cloud Storage](https://cloud.google.com/storage) using the [`google-cloud-storage`][google-cloud-storage] client.

A minimal setup for the `BlobStorage` requires a storage client and a "router" function that
tells the storage in what bucket and under what path each piece of content should be
saved:

```python
from google.cloud.storage import Client

from labox.extra.google import BlobStorage
from labox.extra.google import simple_blob_router

blob_storage = BlobStorage(
    storage_client=Client(),
    storage_router=simple_blob_router("my-bucket"),
)
```

The `simple_blob_router` requires a bucket name and an optional `prefix` that blobs
should be added under. If no `storage_router` is provided the storage becomes read-only and
attempts to write to it will produce a `NotImplementedError`.

### Blob Router

A [`BlobRouter`][labox.extra.google.BlobRouter] is a function that returns a
[`BlobPointer`][labox.extra.google.BlobPointer] dict given:

- a [digest](../concepts/storages.md#content-digest)
- a [tag map](../concepts/storages.md#storage-tags)
- and a `temp` flag indicating whether the content being written will be deleted after
    use (this happens for [streaming uploads](#cloud-storage-streamed-uploads)).

The `simple_blob_router` creates paths of the form:

```
{prefix}/{content_hash}{ext}
```

Where:

- `prefix` is the optionally provided path prefix
- `content_hash` comes from the `Digest` passed to the router function
- `ext` is the file extension inferred from the `content_type` within the `Digest`

In the case the `temp` flag is true `simple_blob_router` creates a path of the form:

```
temp/{uuid}{ext}
```

Where `uuid` is a [`uuid4`][uuid.uuid4] hex string.

### Cloud Storage Streamed Uploads

When data is being streamed to a `BlobStorage`, the content is first uploaded to a
temporary location within the bucket. Since the hash of the content is not known
upfront, a temporary location is requested from the storage's
[router](#blob-router) by passing it `temp=True`. When the upload is complete a final
location is requested from the router without the `temp` flag. The data is then copied
from the temporary location to the final location using Google Cloud Storage's server-side
copy operation with generation preconditions to avoid race conditions, and the temporary
blob is deleted.

### Cloud Storage Details

The `BlobStorage` class provides a native `async` interface. However, since the
`google-cloud-storage` client is synchronous, each call to client methods are run in
threads using [`anyio.to_thread.run_sync`][anyio.to_thread.run_sync]. To limit the
number of threads spawned use the `max_readers` and `max_writers` parameters when
constructing the `BlobStorage` instance.

The storage supports customizable chunk sizes via the `object_chunk_size` parameter
and allows overriding the default reader and writer types through the `reader_type`
and `writer_type` parameters for advanced use cases. Metadata from storage tags is
automatically applied to blobs for enhanced organization and retrieval.
