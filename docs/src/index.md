# Lakery

A data storage framework for Python.

## Installation

```bash
pip install lakery  # core package
```

```bash
pip install lakery[all]  # all built-in extras
```

See [integrations](integrations.md) for a list of available extras.

## Basic Usage

Setup your database engine:

Pick your storages and serializers:

```python
from lakery.core import Registries
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.os import FileStorage

registries = Registries(
    serializers=SerializerRegistry([JsonSerializer()]),
    storages=StorageRegistry([FileStorage("temp", mkdir=True)]),
)

registries.begin_context()
```

!!! note

    Using `Registries.context()` is generally preferred for production code.
    `Registries.begin_context()` best used for quick prototyping and testing.

Store and retrieve data:

```python
from lakery.core import Scalar
from lakery.core import data_loader
from lakery.core import data_saver

data = Scalar({"hello": "world"})

with data_saver() as saver:
    saver.save_soon("my_data", data)

with data_loader() as loader:
    future = loader.load_soon("my_data")

assert future.result() == data
```
