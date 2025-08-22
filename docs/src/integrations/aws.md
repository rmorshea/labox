# AWS

!!! note

    Install with `pip install labox[aws]`

## S3 Storage

The [`S3Storage`][labox.extra.aws.S3Storage] provides a
[storage](../concepts/storages.md) implementation backed by
[S3](https://aws.amazon.com/s3/) using the [`boto3`][boto3] client.

A minimal setup for the `S3Storage` requires an S3 client and a "router" function that
tells the storage in what bucket and under what path each piece of content should be
saved:

```python
import boto3

from labox.extra.aws import S3Storage
from labox.extra.aws import simple_s3_router

s3_storage = S3Storage(
    s3_client=boto3.client("s3"),
    s3_router=simple_s3_router("my-bucket"),
)
```

The `simple_s3_router` requires a bucket name and an optional `prefix` that objects
should be added under. If no `s3_router` is provided the storage becomes read-only and
attempts to write to it will produce a `NotImplementedError`.

### S3 Router

An [`S3Router`][labox.extra.aws.S3Router] is a function that returns an
[`S3Pointer`][labox.extra.aws.S3Pointer] dict given:

- a [digest](../concepts/storages.md#content-digest)
- a [tag map](../concepts/storages.md#storage-tags)
- and a `temp` flag indicating whether the content being written will be deleted after
    use (this happens for [multipart uploads](#s3-streamed-uploads)).

The `simple_s3_router` creates paths of the form:

```
{prefix}/{content_hash}{ext}
```

Where:

- `prefix` is the optionally provided path prefix
- `content_hash` comes from the `Digest` passed to the router function
- `ext` is the file extension inferred from the `content_type` within the `Digest`

In the case the `temp` flag is true `simple_s3_router` creates a path of the form:

```
{uuid}{ext}
```

Where `uuid` is a [`uuid4`][uuid.uuid4].

### S3 Streamed Uploads

When data is being streamed to an `S3Storage` a
[multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
is initiated. This allows large files to be uploaded in chunks. Since the hash of the
content is not known upfront, a temporary location is requested from the storage's
[router](#s3-router) by passing it `temp=True`. When the upload is complete a final
location is requested from the router without the `temp` flag. The data is then copied
from the temporary location to the final location.

### S3 Storage Details

The `S3Storage` class provides a native `async` interface. However, since `boto3` is
synchronous, each call to `boto3` methods are run in threads using
[`anyio.to_thread.run_sync`][anyio.to_thread.run_sync]. To limit the number of threads
spawned use the `max_readers` and `max_writers` parameters when constructing the
`S3Storage` instance.
