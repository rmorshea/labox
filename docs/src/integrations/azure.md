# Azure

!!! note

    Install with `pip install labox[azure]`

## Blob Storage

The [`BlobStorage`][labox.extra.azure.BlobStorage] provides a
[storage](../concepts/storages.md) implementation backed by
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) using
the [`azure-storage-blob`][azure-storage-blob] client.

A minimal setup for the `BlobStorage` requires a blob service client and a "router"
function that tells the storage in what container and under what path each piece of
content should be saved:

```python
from azure.storage.blob.aio import BlobServiceClient

from labox.extra.azure import BlobStorage
from labox.extra.azure import simple_blob_router

blob_storage = BlobStorage(
    service_client=BlobServiceClient(
        account_url="https://myaccount.blob.core.windows.net",
        credential="<your-credential>"
    ),
    blob_router=simple_blob_router("my-container"),
)
```

The `simple_blob_router` requires a container name and an optional `prefix` that blobs
should be added under. If no `blob_router` is provided the storage becomes read-only and
attempts to write to it will produce a `NotImplementedError`.

### Blob Router

A [`BlobRouter`][labox.extra.azure.BlobRouter] is a function that returns a
[`BlobPointer`][labox.extra.azure.BlobPointer] dict given:

-   a [digest](../concepts/storages.md#content-digest)
-   a [tag map](../concepts/storages.md#storage-tags)
-   and a `temp` flag indicating whether the content being written will be deleted after
    use (this happens for [streaming uploads](#blob-streamed-uploads)).

The `simple_blob_router` creates paths of the form:

```
{prefix}/{content_hash}{ext}
```

Where:

-   `prefix` is the optionally provided path prefix
-   `content_hash` comes from the `Digest` passed to the router function
-   `ext` is the file extension inferred from the `content_type` within the `Digest`

In the case the `temp` flag is true `simple_blob_router` creates a path of the form:

```
temp/{uuid}{ext}
```

Where `uuid` is a [`uuid4`][uuid.uuid4] hex string.

### Blob Streamed Uploads

When data is being streamed to a `BlobStorage`, the content is first uploaded to a
temporary location within the container. Since the hash of the content is not known
upfront, a temporary location is requested from the storage's [router](#blob-router) by
passing it `temp=True`. When the upload is complete a final location is requested from
the router without the `temp` flag. The data is then copied from the temporary location
to the final location using Azure's server-side copy operation, and the temporary blob
is deleted.
