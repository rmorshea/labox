# <img src="./logo.svg" alt="Labox Logo" style="height:2em;position:relative;top:0.4em"> Labox

A storage framework for heterogeneous data in Python.

## At a Glance

Model your data with Pydantic

```python
import pandas as pd
import plotly.express as px
from labox.extra.pydantic import StorableModel


class ExperimentData(StorableModel, class_id="..."):  # (1)!
    name: str
    params: dict[str, float]
    data: pd.DataFrame
    plot: go.Figure


# Populate it with data
params = {"temperature": 298.15, "ph": 7.4, "concentration": 0.1}
data = pd.DataFrame({"measure_num": [1, 2, 3], "measure_val": [1.23, 4.56, 7.89]})
plot = px.scatter(data)
experiment = ExperimentData(name="Measurement data", params=params, data=data, plot=plot)
```

1. Models have immutable `class_id`s [you need to define](./concepts/storables.md#class-ids).

Then save and load it to a [storage backend](./concepts/storables.md) of your choice:

```python
import boto3
from sqlalchemy.ext.asyncio import AsyncSession
from labox.core import Registry, save_one, load_one
from labox.extra.aws import S3Storage, simple_s3_router

# With a bit of setup
s3_storage = S3Storage(boto3.client("s3"), simple_s3_router("my-bucket"))
registry = Registry(...)  # (1)!
session = AsyncSession(...)  # (2)!

record = await save_one(experiment, session=session, registry=registry)
loaded_experiment = await load_one(record, ExperimentData, session=session, registry=registry)
```

1. Labox has a registry of integrations [you need to declare](./usage/index.md#registry-setup)
1. Labox uses SQLAlchemy sessions [you need to be establish](./usage/index.md#database-setup)

## Installation

To install the core package:

```bash
pip install labox
```

To include the core package and extra integrations:

```bash
# all extras
pip install labox[all]

# or specific extras
pip install labox[pydantic,pandas,aws]
```

Be sure to checkout how Labox works with [Pydantic](./integrations/pydantic.md) as well
as all the other [integrations](./integrations/index.md) Labox offers.

## Next Steps

Check out more [usage examples](./usage/index.md), or dive into the
[concepts](./concepts/index.md) and [integrations](./integrations/index.md) to learn
more about how Labox works.
