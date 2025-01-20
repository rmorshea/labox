# Models

Lakery relies on "models" to define where and how to store your data. Models are classes
inheriting from [`BaseStorageModel`][lakery.core.model.BaseStorageModel]:

```python
from typing import Self

from lakery.core.model import BaseStorageModel


class MyModel(BaseStorageModel):
    # (1) String containing a UUID that uniquely identifies the model.
    storage_model_id = "..."

    # (2) Method returning manifests that describe where and how to store the model.
    def storage_model_dump(self, registries: Registries) -> ManifestMap: ...

    # (3) Method that reconstitutes the model from manifests it previously saved.
    @classmethod
    def storage_model_load(cls, manifests: ManifestMap, registries: Registries) -> Self: ...
```

1. String containing a UUID that uniquely identifies the model. This is used to later
    determine which class to reconstitute when loading data later. That means you should
    **never copy or change this value** once it's been used to save data.

1. Method that returns a mapping of [`Manifest`][lakery.core.model.Manifest] dicts that
    that describe where and how to store the constituent parts of the model. This is
    later passed to the [`storage_model_load`][lakery.core.model.BaseStorageModel.storage_model_load]
    method.

1. Method that takes the mapping of [`Manifest`][lakery.core.model.Manifest] dicts and
    reconstitutes the model from them.

Lakery provides built-in models for
[singular][lakery.core.model.Streamed] and [streamed][lakery.core.model.Streamed] data
in addition to basic support for [dataclasses](../integrations/dataclasses.md). Integrations
with 3rd party libraries like [Pydantic](../integrations/pydantic.md) provide
implementations that understand how to decompose and reconstitute more complex data
structures. You can also create your own models to handle you're own objects.

## Custom Models
