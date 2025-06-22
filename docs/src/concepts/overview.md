# Overview

When Lakery saves a [`Storable`](./storables.md) object, it goes through a series of
steps to convert the object into a format suitable for storage. This process involves
[unpacking](./unpackers.md) the object into its constituent parts,
[serializing](./serializers.md) those parts, and then storing the serialized data in a
backend [storage](./storages.md) system.

<pre class="mermaid">
flowchart LR
    START:::hidden -- Storable Object --> U[Unpacker]
    U -- UnpackedValue --> S1[Serializer]
    U -- UnpackedValue --> S2[Serializer]
    U --> S3[...]
    S1 -- SerializedData --> R1[Storage]
    S2 -- SerializedData --> R2[Storage]
    S3 --> R3[...]
    R1 -- bytes --> C1[(Remote Backend)]
    R2 -- bytes --> C2[(Remote Backend)]
    R3 --> C3:::hidden
    classDef hidden display: none;
</pre>

To faciliate loading a `Storable`, Lakery preserves metadata about the components (i.e.
unpacker, serializers and storages) that were used when it was originally saved. That
metadata lives in several [PostgreSQL database](./database.md) tables. Then, in
conjunction with that metadata, Lakery is able to access those components via a
[registry](./registries.md).

In the diagram below, the necessary input to load a `Storable` is a
[`ManifestRecord`](./database.md#manifest-record) which can be used to query the Lakery
database for associated [`ContentRecord`](./database.md#content-record)s. Each
[`ContentRecord`](./database.md#content-record) contains the serialized data for one of
the constituent parts of the `Storable` object.

<pre class="mermaid">
flowchart LR
    START:::hidden -- ManifestRecord --> D[Database]
    D -- ContentRecord --> R1[Storage]
    D -- ContentRecord --> R2[Storage]
    D --> R3[...]
    R1 -- SerializedData --> S1[Serializer]
    R2 -- SerializedData --> S2[Serializer]
    R3 --> S3[...]
    U[Unpacker]
    S1 -- UnpackedValue --> U
    S2 -- UnpackedValue --> U
    S3 --> U
    U -- Storable --> END:::hidden
    classDef hidden display: none;
</pre>
