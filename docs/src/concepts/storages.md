# Storages

Laykery storages are used to persist data from a [serializer](./serializers.md). All
storage implementations must support persisting singular blobs of data as well as
streams of data when subclassing the [`Storage`][lakery.core.storage.Storage] base
class.
