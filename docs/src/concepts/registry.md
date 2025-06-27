# Registries

Lakery stores [serializers](#adding-serializers), [storages](#adding-storages),
[unpackers](#adding-unpackers) and [storables](#adding-storables) in a registry that
Lakery uses when saving and loading data. For example, when saving data, Lakery infers
the appropriate serializer for a value (if not explicitly specified) based on the set of
serializers that have been registered.

## Adding Storables

You can add [storables](./storages.md) to a registry by passing a list of them to the
[`Registry`][lakery.core.registry.Registry] constructor:

```python
from lakery import Registry
from lakery import StorableValue

reg = Registry(storables=[StorableValue])
```

When you add a storable its associated [unpacker](./unpackers.md) is also added to the
registry.

## Adding Unpackers

If an unpacker is not already registered for a storable, you can add it to the registry
by passing it to the [`Registry`][lakery.core.registry.Registry] constructor:

```python
from lakery import Registry
from lakery import Unpacker

my_unpacker: Unpacker
reg = Registry(unpackers=[my_unpacker])
```

If you add an unpacker explicitly it will take precedence over any unpacker registered
with a storable. Specifically, it will be used to unpack any of its
[`types`][lakery.core.unpacker.Unpacker.types] based on the standard
[type inference](type-inference) logic.

## Adding Serializers

You can add [`Serializer`][lakery.core.serializer.Serializer] and
[`StreamSerializer`][lakery.core.serializer.StreamSerializer] instances to a registry
through the `serializers` argument:

```python
from lakery import Registry
from lakery.builtin import iso8601_serializer
from lakery.builtin import json_serializer

reg = Registry(serializers=[iso8601_serializer, json_serializer])
```

If no serializer is specified when saving a value, Lakery will infer the appropriate one
whether the serializers [`types`][lakery.core.serializer.Serializer.types] match using
[type inference](#type-inference) logic.

### Infering Serializers

## Adding Storages

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

## Loading from Modules

You can load all of the above components from modules by passing the name of the module
to the `Registry` constructor as a string or as a module object:

```python
from lakery import Registry

reg = Registry(modules=["lakery.builtin", "myapp.serializers"])
```

Components are loaded from the module by inspecting the `__all__` attribute of the
module. If a component is not declared in `__all__`, it will not be loaded. This allows
you to control which components are loaded from a module.

## Merging Registries

One or more registries can be merged together using the `registries` argument of the
[`Registry`][lakery.core.registry.Registry] constructor. This allows you to combine
multiple registries into a single one.

```python
from lakery import Registry

reg1: Registry
reg2: Registry
reg3: Registry

reg4 = Registry(
    registries=[reg1, reg2, reg3],
    serializers=[my_serializer],  # Optional additional serializers
    storables=[my_storable],  # Optional additional storables
)
```

## Type Inference

The logic for matching a serializer to a value or an unpacker to a storable is based on
the following psudo-code snippet:

```python
def infer_serializer(cls: type, serializers: dict[type, Serializer]) -> Serializer:
    for base in cls.__mro__:
        if base in serializers:
            return serializers[base]
    raise ValueError


def infer_unpacker(cls: type, unpackers: dict[type, Unpacker]) -> Unpacker:
    for base in cls.__mro__:
        if base in unpackers:
            return unpackers[base]
    raise ValueError
```

The serializer and unpacker mappings are constructed from the
[`types`][lakery.core.serializer.Serializer.types] and
[`types`][lakery.core.unpacker.Unpacker.types] attributes respectively.

Type inference can be disabled by setting either `infer_serializers=False` or
`infer_unpackers=False` when creating a [`Registry`][lakery.core.registry.Registry].

## Precedence

When multiple serializers, storages or unpackers are registered, the last one in the
list will take precedence. This means that if you register a serializer, storage or
unpacker with the same name as an existing one, it will override the existing one. This
is also true for [type inference](#type-inference) where the last serializer

The order of precedence amongst modules, registries, and explicitly provided keyword
arguments is, in increasing order of priority:

1. Modules loaded from the `modules` argument.
1. Registries merged from the `registries` argument.
1. Explicitly provided serializers, storages, unpackers, or storables in the
    `serializers`, `storages`, `unpackers`, or `storables` arguments.
