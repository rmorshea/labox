# Pydantic

!!! note

    Install with `pip install labox[pydantic]`

The Pydantic integration for Labox provides a way to make you Pydantic models storable.

## Basic Usage

Start by inheriting from the `StorableModel` class and declaring a
[`class_id`](../concepts/storables.md#class-ids). Then define your model as normal:

```python
from datetime import datetime

from labox.extra.pydantic import StorableModel


class ExperimentData(StorableModel, class_id="..."):
    description: str
    started_at: datetime
    results: list[list[int]]
```

## Storable Specs

You can define
[custom types](https://docs.pydantic.dev/latest/concepts/types/#custom-types) that are
[annotated][typing.Annotated] with a [`StorableSpec`][labox.extra.pydantic.StorableSpec]
which describe how and where to store values.

```python
from typing import Annotated
from labox.extra.pydantic import StorableSpec
from labox.builtin import CsvSerializer
from labox.builtin import FileStorage


class ExperimentData(StorableModel, class_id="..."):
    description: str
    started_at: datetime
    results: Annotated[
        list[list[int]],
        StorableSpec(serializer=CsvSerializer, storage=FileStorage),
    ]
```

You can make these custom types
[generic](https://docs.pydantic.dev/latest/concepts/types/#generics) with a
[`TypeVar`][typing.TypeVar]:

```python
from typing import TypeVar

T = TypeVar("T")
SaveAsCsv = Annotated[T, StorableSpec(serializer=CsvSerializer)]
SaveInFile = Annotated[T, StorableSpec(storage=FileStorage)]


class ExperimentData(StorableModel, class_id="..."):
    description: str
    started_at: datetime
    results: SaveInFile[SaveAsCsv[list[list[int]]]]
```
