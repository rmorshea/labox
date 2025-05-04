# Models

Lakery relies on "models" to define where and how to store your data.

## Simple Models

### Singular

The [`Singular`][lakery.common.models.Singular] model can be used to save any value for
which there is a [serializer](./serializers.md). For example, you might have a
`pandas.DataFrame` that you want to save as a Parquet file. All you need to do is wrap
it in a `Singular` instance:

```python
import pandas as pd

from lakery.common.models import Singular

my_df = pd.DataFrame({"hello": ["world"]})
singular_df = Singular(my_df)
```

You can explicitly declare a serializer to use when saving the data.

```python
from lakery.extra.pandas import ParquetDataFrameSerializer

parquet_df_serializer = ParquetDataFrameSerializer()
singular_df = Singular(df, serializer=parquet_df_serializer)
```

If you don't declare a serializer as part of the `Singular` model, Lakery will search
for on in the [serializer registry](./registries.md#serializer-registry) passed to
[`data_saver`](../usage/index.md#saving) later on. The same is true of storages though,
if you're [storage registry](./registries.md#storage-registry), does not have a
[default storage](registries.md#default-storage) then declaring one here is required.

```python
from lakery.extra.os import FileStorage

file_storage = FileStorage("temp", mkdir=True)
singular_df = Singular(df, storage=file_storage)
```

### Streamed

The [`Streamed`][lakery.common.models.Streamed] model can be used to save data that is
asynchronously generated. For example, you might have a function that generates data you
want to save as a Parquet file. You can wrap that asynchronous generator in a `Streamed`
instance:

```python
import pyarrow as pa

from lakery.common.models import Streamed
from lakery.extra.pyarrow import ParquetRecordBatchStreamSerializer


async def generate_data():
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({"count": [i]})
        yield batch


parquet_stream_serializer = ParquetRecordBatchStreamSerializer()
streamed_data = Streamed(generate_data(), serializer=parquet_stream_serializer)
```

!!! warning

    Unlike with `Singular`, passing an explicit serializer to `Streamed` is
    **highly recommended**. This is because determine what serializer to use involves
    waiting for the first element of the stream to be generated. This means will you
    will get a late error when a serializer cannot be found.

As above, if you're storage registry does not have a
[default storage](registries.md#declaring-a-default-storage) then declaring one here is
required.

```python
import pyarrow as pa

from lakery.common.models import Streamed
from lakery.extra.os import FileStorage
from lakery.extra.pyarrow import ParquetRecordBatchStreamSerializer


async def generate_data():
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({"count": [i]})
        yield batch


file_storage = FileStorage("temp", mkdir=True)
parquet_stream_serializer = ParquetRecordBatchStreamSerializer()
streamed_data = Streamed(
    generate_data(),
    serializer=parquet_stream_serializer,
    storage=file_storage,
)
```

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

Every model type has a `storage_model_id` attribute that is used to uniquely identify it
when saving and loading data. This is important because it's how Lakery knows which
class to use when reconstituting data. That means you should **never copy or change this
value** once it's been used to save data. On the other hand you are free to rename the
class or move it to a different module without any issues since the `storage_model_id`,
rather than an "import path", is used to identify the model.

### Generating IDs

Whenever you inherit from [`BaseStorageModel`][lakery.core.model.BaseStorageModel] you
must declare a `storage_model_id` as part of the class definition. However, since these
IDs should be randomly generated, when you first define your model you can use a
placeholder value like `"..."`:

```python
from lakery.core.model import BaseStorageModel


class MyModel(BaseStorageModel, storage_model_id="..."):
    pass
```

Later, when you run this code, Lakery will issue a `UserWarning`:

```
'...' is not a valid storage model ID for MyModel. Try adding <generated-id> to your class definition.
```

You can then use the `<generated-id>` value in place of `"..."` to make it unique.

!!! note

    In the future, Lakery come with a linter that automatically generates unique storage model IDs
    for you as you work.

### Abstract Models

If your class is "abstract" (i.e. direct instances of that class will never be saved)
then you can declare it as `None`:

```python
from lakery.core.model import BaseStorageModel


class MyAbstractModel(BaseStorageModel, storage_model_id=None):
    pass
```
