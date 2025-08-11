# Database

The [PostgreSQL](https://www.postgresql.org/) database schema for Labox contains two
types of records:

-   [Manifest Records](#manifest-records): Contains metadata about a stored object and
    its associated content.
-   [Content Records](#content-records): Pointers to the actual content of the stored
    object.

Each manifest record corresponds to a single [`Storable`](./storables.md) object while
each content record related to a manifest corresponds to the content deconstructed by an
[unpacker](./unpackers.md).

## Manifest Records

The `labox_manifests` table contains metadata about stored objects.

| Column Name                                                         | Type        | Description                                                               |
| ------------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------- |
| [`id`][labox.core.database.ManifestRecord.id]                       | `UUID`      | Unique identifier for the manifest record.                                |
| [`created_at`][labox.core.database.ManifestRecord.created_at]       | `TIMESTAMP` | Timestamp when the manifest was created.                                  |
| [`class_id`][labox.core.database.ManifestRecord.class_id]           | `UUID`      | See [Class IDs](./storables.md#class-ids) for more information.           |
| [`unpacker_name`][labox.core.database.ManifestRecord.unpacker_name] | `TEXT`      | See [Unpacker Names](./unpackers.md#unpacker-names) for more information. |
| [`tags`][labox.core.database.ManifestRecord.tags]                   | `JSONB`     | Tags associated with the stored object, stored as a JSONB object.         |

A single manifest record is saved in the `labox_manifests` table whenever you save a
`Storable` object. When you do, you have the option of providing `tags` which will end
up added to the `tags` column. In addition the object's
[class ID](./storables.md#class-ids) and the name of the [unpacker](./unpackers.md) used
to deconstructed will be stored under the `class_id` and `unpacker_name` columns,
respectively.

## Content Records

The `labox_contents` table contains pointers to the actual content of stored objects.

| Column Name                                                                          | Type        | Description                                                                                                                           |
| ------------------------------------------------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [`id`][labox.core.database.ContentRecord.id]                                         | `UUID`      | Unique identifier for the content record.                                                                                             |
| [`created_at`][labox.core.database.ContentRecord.created_at]                         | `TIMESTAMP` | Timestamp when the content record was created.                                                                                        |
| [`manifest_id`][labox.core.database.ContentRecord.manifest_id]                       | `UUID`      | Unique identifier for the related manifest record representing the stored object.                                                     |
| [`content_key`][labox.core.database.ContentRecord.content_key]                       | `TEXT`      | Unique amongst all content records for a given manifest. Given by the unpacker of the content.                                        |
| [`content_type`][labox.core.database.ContentRecord.content_type]                     | `TEXT`      | The [MIME type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types) of the content.                          |
| [`content_encoding`][labox.core.database.ContentRecord.content_encoding]             | `TEXT`      | The encoding of the content, if applicable (e.g., `gzip`, `deflate`).                                                                 |
| [`content_size`][labox.core.database.ContentRecord.content_size]                     | `BIGINT`    | The size of the content in bytes.                                                                                                     |
| [`content_hash`][labox.core.database.ContentRecord.content_hash]                     | `TEXT`      | A hash of the content.                                                                                                                |
| [`content_hash_algorithm`][labox.core.database.ContentRecord.content_hash_algorithm] | `TEXT`      | The algorithm used to compute the content hash (e.g., `sha256`).                                                                      |
| [`serializer_config`][labox.core.database.ContentRecord.serializer_config]           | `JSONB`     | See [Serializer Config](./serializers.md#serializer-config) for more information.                                                     |
| [`serializer_name`][labox.core.database.ContentRecord.serializer_name]               | `TEXT`      | See [Serializer Names](./serializers.md#serializer-names) for more information.                                                       |
| [`serializer_type`][labox.core.database.ContentRecord.serializer_type]               | `ENUM`      | Indicates whether the serializer is a [stream](./serializers.md#stream-serializers) or a [value](./serializers.md#basic-serializers). |
| [`storage_name`][labox.core.database.ContentRecord.storage_name]                     | `TEXT`      | See [Storage Names](./storages.md#storage-names) for more information.                                                                |
| [`storage_config`][labox.core.database.ContentRecord.storage_config]                 | `JSONB`     | See [Storage Config](./storages.md#storage-config) for more information.                                                              |

A content record is saved in the `labox_contents` table for each
[unpacked value](./unpackers.md#unpacked-values) or
[unpacked stream](./unpackers.md#unpacked-streams) returned by an unpacker when
deconstructing a `Storable` object. The `content_key` of each record comes from the keys
in the mapping returned by the
[`unpack_object`][labox.core.unpacker.Unpacker.unpack_object] method.
