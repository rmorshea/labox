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

The model registry is used to map
[storage model IDs](../concepts/models.md#storage-model-ids) to Python classes. This
allows Lakery to know which class to instantiate when loading data.

## Serializer Registry

## Storage Registry

### Default Storage
