# Models

Lakery relies on "models" to define where and how to store your data. There are a few
built-in models for [singular][lakery.common.model.Streamed] and [streamed][lakery.common.model.Streamed]
data in addition to basic support for [dataclasses](#dataclass-model). These built-in
models may be sufficient for simple use cases, but you'll pretty quickly reach for
integrations with 3rd party libraries like [Pydantic](../integrations/pydantic.md),
or start thinking about creating your own [custom models](#custom-models).

## Simple Models

### Singular

The [`Singular`][lakery.common.model.Singular] model can be used to save any value you
already have in memory. For example, you might have a `pandas.DataFrame` that you want
to save as a Parquet file. All you need to do is wrap it in a `Singular` instance:

```python
import pandas as pd

from lakery.common.model import Singular

my_df = pd.DataFrame({"hello": ["world"]})
singular_df = Singular(my_df)
```

Lakery will do its best to infer what serializer to when you save. If you want to be
explicit, you can pass a `serializer` argument to the `Singular` constructor. Doing
so is generally recommended in order to avoid any ambiguity or unexpected behavior.

```python
from lakery.extra.pandas import ParquetDataFrameSerializer

parquet_df_serializer = ParquetDataFrameSerializer()  # (1)!
singular_df = Singular(df, serializer=parquet_df_serializer)
```

The same is true of storages. If you're storage registry does not have a
[default storage](registries.md#declaring-a-default-storage) then declaring one here
is required.

```python
from lakery.extra.os import FileStorage

file_storage = FileStorage("temp", mkdir=True)  # (1)!
singular_df = Singular(df, storage=file_storage)
```

### Streamed

The [`Streamed`][lakery.common.model.Streamed] model can be used to save data that is
asynchronously generated. For example, you might have a function that generates data
you want to save as a Parquet file. You can wrap that asynchronous generator in a
`Streamed` instance:

```python
import pyarrow as pa

from lakery.common.model import Streamed
from lakery.extra.pyarrow import ParquetRecordBatchStreamSerializer


async def generate_data():
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({"count": [i]})
        yield batch


parquet_stream_serializer = ParquetRecordBatchStreamSerializer()  # (1)!
streamed_data = Streamed(generate_data(), serializer=parquet_stream_serializer)
```

!!! warning

    Unlike with `Singular`, passing an explicit serializer to `Streamed` is
    **highly recommended**. This is because determine what serializer to use involves
    waiting for the first element of the stream to be generated. This means will
    not get an early error if a serializer cannot be found.

As above, if you're storage registry does not have a [default storage](registries.md#declaring-a-default-storage)
then declaring one here is required.

```python
import pyarrow as pa

from lakery.common.model import Streamed
from lakery.extra.os import FileStorage
from lakery.extra.pyarrow import ParquetRecordBatchStreamSerializer


async def generate_data():
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({"count": [i]})
        yield batch


file_storage = FileStorage("temp", mkdir=True)
parquet_stream_serializer = ParquetRecordBatchStreamSerializer()
streamed_data = Streamed(
    generate_data(), serializer=parquet_stream_serializer, storage=file_storage
)
```

### Dataclasses

The [`DataclassModel`][lakery.extra.dataclasses.DataclassModel] model can be used to save standard
Python dataclasses. For example, you might have a dataclass that holds the results of a scientific
experiment:

```python
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class ExperimentResults:
    measurements: pd.DataFrame
    image: np.ndarray
```

To turn this into a model that can be saved with Lakery, you need to inherit from
`DataclassModel` and declare a `storage_id` as part of the class definition:

```python
from dataclasses import dataclass
from dataclasses import field

from lakery.extra.dataclasses import DataclassModel
from lakery.extra.numpy import NpySerializer
from lakery.extra.pandas import DataFrameSerializer

npy_serializer = NpySerializer()
df_serializer = DataFrameSerializer()


@dataclass
class ExperimentResultsModel(DataclassModel, storage_id="..."):
    measurements: pd.DataFrame = field(metadata={"serializer": df_serializer})
    image: np.ndarray = field(metadata={"serializer": npy_serializer})
```

!!! note

    The `storage_id` is the same as the [storage_model_id](#storage-model-ids) of a model.

Note how the serializer for each field was specified through the `metadata` argument of
the `field` function. These could be infered automatically when serializing the model
but this takes time and can be unreliable so, particularly in the case of dataclass
models where the structure is known ahead of time, it's recommended to be explicit.
Storages may also be specified in the same way under a `"storage"` key in the `metadata`.

## Custom Models

Implementing your own model involved inheriting from the
[`BaseStorageModel`][lakery.core.model.BaseStorageModel] class.

Assume you have a already have a class that holds data from a scientific experiment:

```python
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go


class ExperimentResults:
    timestamp: datetime
    raw_data: pd.DataFrame
    camera_image: np.ndarray
    analysis: go.Figure
```

This class holds

## Storage Model IDs

The `storage_model_id` attribute of a model is used to uniquely identify it when saving
and loading data. This is important because it's how Lakery knows which class to use
when reconstituting data. That means you should **never copy or change this value**
once it's been used to save data.
