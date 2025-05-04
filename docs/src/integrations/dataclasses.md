# Dataclasses

!!! note

    No extra installation step required.

The [`DataclassModel`][lakery.extra.dataclasses.DataclassModel] model can be used to
save standard Python dataclasses with the restriction that they cannot be nested. If you
need to nest dataclasses you should consider using the Lakery's
[pydantic integration](../integrations/pydantic.md) instead.

For example, you might have a dataclass that holds the results of a scientific
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
`DataclassModel` and declare a [`storage_model_id`](#storage-model-ids) as part of the
class definition:

```python
from dataclasses import dataclass
from dataclasses import field

from lakery.extra.dataclasses import DataclassModel
from lakery.extra.numpy import NpySerializer
from lakery.extra.pandas import DataFrameSerializer

npy_serializer = NpySerializer()
df_serializer = DataFrameSerializer()


@dataclass
class ExperimentResultsModel(DataclassModel, storage_model_id="..."):
    measurements: pd.DataFrame = field(metadata={"serializer": df_serializer})
    image: np.ndarray = field(metadata={"serializer": npy_serializer})
```

Note how the serializer for each field was specified through the `metadata` argument of
the `field` function. These could be infered automatically when serializing the model
but this takes time and can be unreliable. So, particularly in the case of dataclass
models where the structure is known ahead of time, it's recommended to be explicit.
Storages may also be specified in the same way under a `"storage"` key in the
`metadata`.
