# Overview

Labox has built-in and third-party [components](../concepts/index.md) which can be added to a
[registry](../concepts/registry.md) that you pass when [saving](../usage/framework.md#saving-storables)
and [loading](../usage/framework.md#loading-storables) storables. For example, the following example
creates a registry with built-in Labox components as well as those for
[Pandas](./pandas.md) and [Pydantic](./pydantic.md) support:

```python
from labox.builtin import FileStorage
from labox.core import Registry

registry = Registry(
    modules=["labox.builtin", "labox.extra.pandas", "labox.extra.pydantic"],
    default_storage=FileStorage("temp", mkdir=True),
)
```
