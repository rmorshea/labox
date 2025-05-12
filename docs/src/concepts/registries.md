# Registries

Lakery stores serializers, storages, and models in registries to provide a consistent
interface for accessing and managing these components. A
[collection](#registry-collection) of these registries must be passed to Lakery whenever
saving or loading data.

## Registry Collection

A registry collection holds individual registries for each component type. When saving
data, a collection is used to infer a serializer and/or use a default storage. Later,
when loading data, a collection is used to determine where to access the data from and
how to deserialize it and populate the appropriate model.

```python
from lakery.core import ModelRegistry
from lakery.core import RegistryCollection
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry

registries = RegistryCollection(
    models=ModelRegistry(...),
    serializers=SerializerRegistry(...),
    storages=StorageRegistry(...),
)
```

While registry collections are immutable, you can however create new ones by merging
with existing collections.

```python
reg1 = RegistryCollection(...)
reg2 = RegistryCollection(...)
reg3 = reg1.merge(reg2)
```

Or with existing component-level registries.

```python
reg4 = reg3.merge(
    serializers=SerializerRegistry(...),
    storages=StorageRegistry(...),
    models=ModelRegistry(...),
)
```

## Model Registry

A model registry is used to map
[storage model IDs](../concepts/models.md#storage-model-id) to Python classes. This
allows Lakery to know which class to instantiate when loading data.

```python
from lakery.common.models import Singular
from lakery.common.models import Streamed
from lakery.core import ModelRegistry

models = ModelRegistry([Singular, Streamed])
```

### Loading Models from Modules

You can also load models into a registry by module name using the
[`from_modules`][lakery.core.registries.ModelRegistry.from_modules] method. The module
must define an `__all__` variable which includes the models that should be loaded. Any
non-model classes in `__all__` will be ignored.

```python
from lakery.core import ModelRegistry

models = ModelRegistry.from_modules("lakery.common.models")
```

## Serializer Registry

A serializer registry is used to map serializer names to serializer instances. This
allows Lakery to know which serializer to use when saving or loading data.

```python
from lakery.core import SerializerRegistry
from lakery.extra.datetime import Iso8601Serializer
from lakery.extra.numpy import NpySerializer

iso8601_serializer = Iso8601Serializer()
npy_serializer = NpySerializer()

serializers = SerializerRegistry([iso8601_serializer, npy_serializer])
```

### Infering Serializers

You can infer an appropriate serializer for a given type by using either of the methods:

-   [`infer_from_value_type`][lakery.core.registries.SerializerRegistry.infer_from_value_type]
-   [`infer_from_stream_type`][lakery.core.registries.SerializerRegistry.infer_from_stream_type]

If multiple serializers
[declare the same types](./serializers.md#serializer-type-inference) the last serializer
in the list passed to the registry will take precedence.

```python
from datetime import datetime

from lakery.core import SerializerRegistry
from lakery.extra.datetime import Iso8601Serializer
from lakery.extra.numpy import NpySerializer

iso8601_serializer = Iso8601Serializer()
npy_serializer = NpySerializer()

serializers = SerializerRegistry([iso8601_serializer, npy_serializer])

assert isinstance(serializer.infer_from_value_type(datetime), Iso8601Serializer)
```

### Loading Serializers from Modules

You can also load serializers into a registry by module name using the
[`from_modules`][lakery.core.registries.SerializerRegistry.from_modules] method. The
module must define an `__all__` variable which includes the serializer instances that
should be loaded. Any non-serializer instances in `__all__` will be ignored.

```python
from lakery.core import SerializerRegistry

serializers = SerializerRegistry.from_modules("lakery.extra.json", "lakery.extra.pandas")
```

## Storage Registry

A storage registry is used to map storage names to storage instances. This allows Lakery
to know which storage to use when saving or loading data.

```python
from lakery.core import StorageRegistry
from lakery.extra.aws import S3Storage
from lakery.extra.os import FileStorage

file_storage = FileStorage("mydir", mkdir=True)
s3_storage = S3Storage(
    s3_client=boto3.client("s3"),
    bucket_name="my-bucket",
    object_key_prefix="some/prefix",
)

storages = StorageRegistry([file_storage, s3_storage])
```

### Default Storage

You can declare a default storage in the registry. This will be be used in the case that
a storage has not otherwise been specified.

```python
import boto3

from lakery.core import StorageRegistry
from lakery.extra.aws import S3Storage
from lakery.extra.os import FileStorage

file_storage = FileStorage("mydir", mkdir=True)
s3_storage = S3Storage(
    s3_client=boto3.client("s3"),
    bucket_name="my-bucket",
    object_key_prefix="some/prefix",
)

storages = StorageRegistry([file_storage], default=s3_storage)
```

### Loading Storages from Modules

You can also load storages into a registry by module name using the
[`from_modules`][lakery.core.registries.StorageRegistry.from_modules] method. The module
must define an `__all__` variable which includes the storage instances that should be
loaded. Any non-storage instances in `__all__` will be ignored.

```python
from lakery.core import StorageRegistry

storages = StorageRegistry.from_modules("some.module")
```
