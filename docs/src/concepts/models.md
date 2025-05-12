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

While not required, it's recommended to explicitly declare a serializer as well.

```python
from lakery.extra.pandas import ParquetDataFrameSerializer

parquet_df_serializer = ParquetDataFrameSerializer()
singular_df = Singular(df, serializer=parquet_df_serializer)
```

If you don't declare a serializer as part of the `Singular` model, Lakery will search
for on in the [serializer registry](./registries.md#serializer-registry) passed to
[`data_saver`](../usage/index.md#saving) later on. Similarly, if a storage has not been
explicitely declared, the [default storage](registries.md#default-storage) will be used.
If your [storage registry](./registries.md#storage-registry) does not have a default,
declaring an explicit storage is required.

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

To save more complicated objects you can define your own custom models by implementing
the [`BaseStorageModel`][lakery.core.model.BaseStorageModel] interface. Lakery provides
a number of integrations with existing frameworks and libraries to make this easier. For
example:

-   [Dataclasses](../integrations/dataclasses.md)
-   [Pydantic](../integrations/pydantic.md)

### Example Custom Model

Consider the case of class that holds heterogeneous data from a scientific experiment:

```python
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go


class ExperimentResults:
    """Results from a scientific experiment."""

    def __init__(
        self,
        timestamp: datetime,
        timeseries_data: pd.DataFrame,
        camera_image: np.ndarray,
        analysis: go.Figure,
    ):
        self.timestamp = timestamp
        self.timeseries_data = timeseries_data
        self.camera_image = camera_image
        self.analysis = analysis
```

You'll then start to convert this into a model by first inheriting from
`BaseStorageModel` and adding a `storage_model_config`:

```python
from lakery.core import BaseStorageModel


class ExperimentResults(BaseStorageModel, storage_model_config={"id": "abc123", "version": 1}):
    """Results from a scientific experiment."""

    ...
```

You'll then need to the set of serializers that are required to encode each type of data
in your model. In this case you could use:

```python
from lakery.extra.datetime import Iso8601Serializer
from lakery.extra.numpy import NpySerializer
from lakery.extra.pandas import ParquetDataFrameSerializer
from lakery.extra.plotly import FigureSerializer

iso8601_serializer = Iso8601Serializer()
parquet_df_serializer = ParquetDataFrameSerializer()
npy_serializer = NpySerializer()
figure_serializer = FigureSerializer()
```

Next you'll then need to implement the `storage_model_dump` method. To do so you'll need
to return a [`Content`][lakery.core.model.Content] dictionary for each field in your
model given a [`RegistryCollection`](registries.md):

```python
class ExperimentResults(BaseStorageModel, storage_model_config={"id": "abc123", "version": 1}):
    """Results from a scientific experiment."""

    ...

    def storage_model_dump(self, registries: RegistryCollection) -> dict[str, Content]:
        return {
            "timestamp": {
                "value": self.timestamp,
                "serializer": iso8601_serializer,
                "storage": None,
            },
            "timeseries_data": {
                "value": self.timeseries_data,
                "serializer": parquet_df_serializer,
                "storage": None,
            },
            "camera_image": {
                "value": self.camera_image,
                "serializer": npy_serializer,
                "storage": None,
            },
            "analysis": {
                "value": self.analysis,
                "serializer": figure_serializer,
                "storage": None,
            },
        }
```

And finally you'll need to implement the `storage_model_load` method which accepts the
same `Content` dictionary as the `storage_model_dump` method in addition to the
[model version](#storage-model-version) that saved the data and a
[`RegistryCollection`](registries.md):

```python
from typing import Self


class ExperimentResults(BaseStorageModel, storage_model_config={"id": "abc123", "version": 1}):
    """Results from a scientific experiment."""

    ...

    @classmethod
    def storage_model_load(
        cls,
        content: dict[str, Content],
        version: int,
        registries: RegistryCollection,
    ) -> Self:
        return cls(
            timestamp=content["timestamp"]["value"],
            timeseries_data=content["timeseries_data"]["value"],
            camera_image=content["camera_image"]["value"],
            analysis=content["analysis"]["value"],
        )
```

Putting this all together, you get:

```python
from datetime import datetime
import numpy as np
import pandas as pd
import plotly.graph_objects as go


from lakery.core import BaseStorageModel
from lakery.core.model import AnyStorageValue, StorageValue
from lakery.core.registries import RegistryCollection
from lakery.extra.datetime import Iso8601Serializer
from lakery.extra.numpy import NpySerializer
from lakery.extra.pandas import ParquetDataFrameSerializer
from lakery.extra.plotly import FigureSerializer

iso8601_serializer = Iso8601Serializer()
parquet_df_serializer = ParquetDataFrameSerializer()
npy_serializer = NpySerializer()
figure_serializer = FigureSerializer()


class ExperimentResults(BaseStorageModel, storage_model_config={"id": "abc123", "version": 1}):
    """Results from a scientific experiment."""

    def __init__(
        self,
        timestamp: datetime,
        timeseries_data: pd.DataFrame,
        camera_image: np.ndarray,
        analysis: go.Figure,
    ):
        self.timestamp = timestamp
        self.timeseries_data = timeseries_data
        self.camera_image = camera_image
        self.analysis = analysis

    def storage_model_dump(self, registries: RegistryCollection) -> dict[str, StorageValue]:
        return {
            "timestamp": {
                "value": self.timestamp,
                "serializer": iso8601_serializer,
                "storage": None,
            },
            "timeseries_data": {
                "value": self.timeseries_data,
                "serializer": parquet_df_serializer,
                "storage": None,
            },
            "camera_image": {
                "value": self.camera_image,
                "serializer": npy_serializer,
                "storage": None,
            },
            "analysis": {
                "value": self.analysis,
                "serializer": figure_serializer,
                "storage": None,
            },
        }

    @classmethod
    def storage_model_load(
        cls,
        content: dict[str, StorageValue],
        version: int,
        registries: RegistryCollection,
    ) -> Self:
        return cls(
            timestamp=content["timestamp"]["value"],
            timeseries_data=content["timeseries_data"]["value"],
            camera_image=content["camera_image"]["value"],
            analysis=content["analysis"]["value"],
        )
```

## Storage Model Config

Every model type must declare a `storage_model_config` when it's defined.

```python
from lakery.core.model import BaseStorageModel


class MyModel(BaseStorageModel, storage_model_config={"id": "abc123", "version": 1}):
    pass
```

### Storage Model ID

The `id` within the config for a storage model uniquely identify it when saving and
loading data. This is important because it's how Lakery knows which class to use when
reconstituting data. That means you should **never copy or change this value** once it's
been used to save data. On the other hand you are free to rename the class or move it to
a different module without any issues since the `id`, rather than an "import path", is
used to identify the model.

#### Generating IDs

Whenever you inherit from [`BaseStorageModel`][lakery.core.model.BaseStorageModel] you
must declare a `storage_model_id` as part of the class definition. However, since these
IDs should be randomly generated, when you first define your model you can use a
placeholder value like `"..."`:

```python
from lakery.core.model import BaseStorageModel


class MyModel(BaseStorageModel, storage_model_config={"id": "...", "version": 1}):
    pass
```

Later, when you run this code, Lakery will issue a `UserWarning`:

```txt
Ignoring storage model config for '__main__.MyModel' - '...' is not a valid storage
model ID. Expected 8-16 character hexadecimal string. Try adding '<generated-id>' to
your class definition instead.
```

You can then use the `'<generated-id>'` value in place of `'...'` to make it unique.

!!! note

    In the future, Lakery come with a linter that automatically generates unique storage model IDs
    for you as you work.

### Storage Model Version

The `version` within the config for a storage model is saved with a model's data. Later,
when that data is reconstituted, the version will be passed to the model's
`storage_model_load` method. This allows the author of the model to handle any changes
to the model definition that may have occurred since the data was saved. For example,
you might have changed the name of a field.

### Abstract Models

If your class is "abstract" (i.e. direct instances of that class will never be saved)
then you can declare the storage model config to be `None`:

```python
from lakery.core.model import BaseStorageModel


class MyAbstractModel(BaseStorageModel, storage_model_config=None):
    pass
```
