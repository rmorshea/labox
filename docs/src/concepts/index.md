# Overview

When Lakery saves a [`Storable`](./storables.md) object, it goes through a series of
steps to convert the object into a format suitable for storage. This process involves
[unpacking](./unpackers.md) the object into its constituent parts,
[serializing](./serializers.md) those parts, and then storing the serialized data in a
backend [storage](./storages.md) system.

<pre class="mermaid" style="min-width: 100%;">
flowchart LR
    START:::hidden -- Storable Object --> U[Unpacker]
    U -- UnpackedValue --> S1[Serializer]
    U -- UnpackedValue --> S2[Serializer]
    U --> S3[...]
    S1 -- SerializedData --> R1[Storage]
    S2 -- SerializedData --> R2[Storage]
    S3 --> R3[...]
</pre>

To faciliate loading a `Storable`, Lakery uses a [PostgreSQL database](./database.md) to
preserve metadata about the components that were used when it was originally saved (i.e.
the unpackers, serializers and storages). Lakery then uses that metadata to pick the
component instance out of a [registry](./registry.md) when needed.

In the diagram below, the necessary input to load a `Storable` is a
[`ManifestRecord`](./database.md#manifest-records) which can be used to query the Lakery
database for associated [`ContentRecord`](./database.md#content-records)s. Each
`ContentRecord` is a pointer to a piece of data that was part of the original
`Storable`.

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
