# Overview

When Lakery saves a [`Storable`](./storables.md) object, it goes through a series of
steps to convert the object into a format suitable for storage. This process involves
[unpacking](./unpackers.md) the object into its constituent parts,
[serializing](./serializers.md) those parts, and then storing the serialized data in a
backend [storage](./storages.md) system. At the end of this process, a
[`ManifestRecord`](./database.md#manifest-records) is created in the Lakery database to
preserve metadata about the components used in the process.

<pre class="mermaid">
flowchart LR
    START:::hidden -- Storable --> U[Unpacker]
    U -- UnpackedValue --> S1[Serializer]
    U -- UnpackedValue --> S2[Serializer]
    U --> S3[...]
    S1 -- SerializedData --> R1[Storage]
    S2 -- SerializedData --> R2[Storage]
    S3 --> R3[...]
    classDef hidden display: none;
</pre>

To faciliate loading a `Storable`, the user must provide the `ManifestRecord` that was
created when the `Storable` object was saved. Lakery uses this manifest to find its
associated [`ContentRecord`](./database.md#content-records)s, which point to the
serialized data in the storage system. Lakery then retrieves the data and the various
components (unpackers, serializers, and storages) by their names from a
[registry](./registry.md) to reconstruct the original `Storable` object.
