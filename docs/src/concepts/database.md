# Database

The [PostgreSQL](https://www.postgresql.org/) database schema for Lakery contains two
types of records:

- [Manifest Records](#manifest-records): Contains metadata about a stored object and
    its associated content.
- [Content Records](#content-records): Pointers to the actual content of the stored
    object.

Each manifest record corresponds to a single [`Storable`](./storables.md) object while
each content record related to a manifest corresponds to the content deconstructed by an
[unpacker](./unpackers.md).

## Manifest Records

The `lakery_manifests` table contains metadata about stored objects.

| Column Name                                                          | Type        | Description                                                       |
| -------------------------------------------------------------------- | ----------- | ----------------------------------------------------------------- |
| [`id`][lakery.core.database.ManifestRecord.id]                       | `UUID`      | Unique identifier for the manifest record.                        |
| [`created_at`][lakery.core.database.ManifestRecord.created_at]       | `TIMESTAMP` | Timestamp when the manifest was created.                          |
| [`class_id`][lakery.core.database.ManifestRecord.class_id]           | `UUID`      | Unique identifier for the class of the stored object.             |
| [`unpacker_name`][lakery.core.database.ManifestRecord.unpacker_name] | `TEXT`      | Name of the unpacker used to deconstruct the stored object.       |
| [`tags`][lakery.core.database.ManifestRecord.tags]                   | `JSONB`     | Tags associated with the stored object, stored as a JSONB object. |

A single manifest record is saved in the `lakery_manifests` table whenever you save a
`Storable` object. When you do, you have the option of providing `tags` which will end
up added to the `tags` column. In addition the object's
[class ID](./storables.md#class-ids) and the name of the [unpacker](./unpackers.md) used
to deconstructed will be stored under the `class_id` and `unpacker_name` columns,
respectively.

## Content Records

The `lakery_contents` table contains pointers to the actual content of stored objects.

| Column Name                                                                           | Type        | Description                                                                                                                           |
| ------------------------------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [`id`][lakery.core.database.ContentRecord.id]                                         | `UUID`      | Unique identifier for the content record.                                                                                             |
| [`created_at`][lakery.core.database.ContentRecord.created_at]                         | `TIMESTAMP` | Timestamp when the content record was created.                                                                                        |
| [`manifest_id`][lakery.core.database.ContentRecord.manifest_id]                       | `UUID`      | Unique identifier for the related manifest record representing the stored object.                                                     |
| [`content_name`][lakery.core.database.ContentRecord.content_name]                     | `TEXT`      | Unique amongst all content records for a given manifest. Given by the unpacker or the content.                                        |
| [`content_type`][lakery.core.database.ContentRecord.content_type]                     | `TEXT`      | The [MIME type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types) of the content.                          |
| [`content_encoding`][lakery.core.database.ContentRecord.content_encoding]             | `TEXT`      | The encoding of the content, if applicable (e.g., `gzip`, `deflate`).                                                                 |
| [`content_size`][lakery.core.database.ContentRecord.content_size]                     | `BIGINT`    | The size of the content in bytes.                                                                                                     |
| [`content_hash`][lakery.core.database.ContentRecord.content_hash]                     | `TEXT`      | A hash of the content.                                                                                                                |
| [`content_hash_algorithm`][lakery.core.database.ContentRecord.content_hash_algorithm] | `TEXT`      | The algorithm used to compute the content hash (e.g., `sha256`).                                                                      |
| [`serializer_name`][lakery.core.database.ContentRecord.serializer_name]               | `TEXT`      | The name of the serializer used to serialize the content.                                                                             |
| [`serializer_type`][lakery.core.database.ContentRecord.serializer_type]               | `ENUM`      | Indicates whether the serializer is a [stream](./serializers.md#stream-serializers) or a [value](./serializers.md#basic-serializers). |
| [`storage_name`][lakery.core.database.ContentRecord.storage_name]                     | `TEXT`      | The name of the storage where the content is stored.                                                                                  |
| [`storage_data`][lakery.core.database.ContentRecord.storage_data]                     | `JSONB`     | Data used by the storage to locate the content                                                                                        |
| [`tags`][lakery.core.database.ContentRecord.tags]                                     | `JSONB`     | Tags that were returned with the content by the unpacker.                                                                             |

A content record is saved in the `lakery_contents` table for each
[unpacked value](./unpackers.md#unpacked-values) or
[unpacked stream](./unpackers.md#unpacked-streams) returned by an unpacker when
deconstructing a `Storable` object. The `content_key` of each record comes from the keys
in the mapping returned by the
[`unpack_object`][lakery.core.unpacker.Unpacker.unpack_object] method. Each unpacked
value or stream may also have associated tags, which will be stored in the `tags`
column.
