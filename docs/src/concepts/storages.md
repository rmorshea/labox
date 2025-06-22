# Storages

Laykery storages are used to persist data from a [serializer](./serializers.md). All
storage implementations must support persisting singular blobs of data as well as
streams of data when subclassing the [`Storage`][lakery.core.storage.Storage] base
class.

## Storage Names

Storages are identified by a globally unique name associated with each storage class
within a [registry](./registry.md#adding-storages). Once a storage has been used to
saved data this name must never be changed and the implementation of the storage must
always remain backwards compatible. If you need to change the implementation of a
storage you should create a new one with a new name. In order to continue loading old
data you'll have to preserve and registry the old storage.
