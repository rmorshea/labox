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

from labox.builtin import CsvSerializer
from labox.builtin import FileStorage
from labox.extra.pydantic import StorableSpec


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

Storable specs can be applied to values nested within a field. For example, you might
have a list of images that you want to attach to your model. You could define a custom
type for the images and add the `StorableSpec` to it:

```python
from typing import Annotated

from labox.extra.imageio import Media
from labox.extra.imageio import MediaSerializer
from labox.extra.pydantic import StorableSpec


class ExperimentData(StorableModel, class_id="..."):
    description: str
    started_at: datetime
    images: list[Annotated[Media, StorableSpec(serializer=MediaSerializer)]]
```

## Pydantic Unpacker

A `StorableModel` is saved under one or more
[`ContentRecord`s](../concepts/database.md#content-records). The majority of the model
is stored in a "body" record, which is nominally a JSON serializable object. The
serializer and storage for this body record can be customized by overriding the
[`storable_body_serializer`][labox.extra.pydantic.StorableModel.storable_body_serializer]
and/or
[`storable_body_storage`][labox.extra.pydantic.StorableModel.storable_body_storage]
methods in the dataclass itself. Fields within the dataclass are stored within this same
body record unless the field declares a `storage` or `serializer` that is a
[`StreamSerializer`](../concepts/serializers.md#stream-serializers). In either case
those fields are captured in separate `ContentRecord`s.

To understand how this works in practice, here's what the
[unpacker][labox.extra.pydantic.StorableModelUnpacker] would output for the model below:

```python
from datetime import UTC
from datetime import datetime
from pprint import pprint
from typing import Annotated

from plotly import graph_objs as go

from labox.builtin import CsvSerializer
from labox.builtin import FileStorage
from labox.core import Registry
from labox.extra.plotly import FigureSerializer
from labox.extra.pydantic import StorableModel
from labox.extra.pydantic import StorableSpec


class ExperimentData(StorableModel, class_id="b2138434"):
    description: str
    started_at: datetime
    results: Annotated[
        list[list[int]],
        StorableSpec(serializer=CsvSerializer, storage=FileStorage),
    ]
    figure: Annotated[
        go.Figure,
        StorableSpec(serializer=FigureSerializer),
    ]


exp_data = ExperimentData(
    description="My experiment",
    started_at=datetime.now(UTC),
    results=[[1, 2, 3], [4, 5, 6]],
    figure=go.Figure(data=go.Scatter(y=[1, 3, 2])),
)


unpacker = ExperimentData.storable_config().unpacker

registry = Registry(
    modules=["labox.builtin", "labox.extra.pydantic", "labox.extra.plotly"], default_storage=True
)
unpacked_obj = unpacker.unpack_object(exp_data, registry)
pprint(unpacked_obj)
```

```python
{
    "body": {
        "serializer": JsonSerializer("labox.json.value@v1"),
        "storage": MemoryStorage("labox.memory@v1"),
        "value": {
            "description": "My experiment",
            "figure": {
                "__labox__": "content",
                "content_base64": "eyJkYXRhIjpbeyJ5IjpbMSwzLDJ...",
                "content_encoding": "utf-8",
                "content_type": "application/vnd.plotly.v1+json",
                "serializer_name": "labox.plotly.value@v1",
            },
            "results": {"__labox__": "ref", "ref": "ref.ExperimentData.results.1"},
            "started_at": "2025-08-24T17:11:37.164923Z",
        },
    },
    "ref.ExperimentData.results.1": {
        "serializer": CsvSerializer("labox.csv@v1"),
        "storage": FileStorage("labox.file@v1"),
        "value": [[1, 2, 3], [4, 5, 6]],
    },
}
```

Each item in the resulting dict is an
[`UnpackedValue`](../concepts/unpackers.md#unpacked-values) that would correspond to a
[`ContentRecord`](../concepts/database.md#content-records) in the database. As indicated
earlier the fact that the `results` field of the model had a dedicated storage declared
caused it to be stored separately from the main `body` record.

Within the `body` record the model has been dumped into a JSON-serializable dictionary
containing information about the class as well as its fields. Special `__labox__` keys
within this dictionary are used to store metadata about how each object and/or fields
was dumped. Notably the body contains a reference to `ref.ExperimentData.results.1`
which got unpacked separately.

Fields like `ExperimentData.figure` with custom non-stream serializers are embedded
within the main `body` record to avoid sending a large number of smaller chunks of data
to storage backends. For cloud storage backends having a smaller number of larger
requests tends to be more efficient.
