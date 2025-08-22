# NumPy

!!! note

    Install with `pip install labox[numpy]`

## Array Serializer

The [`NpySerializer`][labox.extra.numpy.NpySerializer] provides a
[serializer](../concepts/serializers.md) implementation for NumPy arrays using the
native `.npy` format. This serializer leverages NumPy's built-in
[`numpy.save`][numpy.save] and [`numpy.load`][numpy.load] functions to efficiently
serialize and deserialize [`numpy.ndarray`][numpy.ndarray] objects.

### Basic Usage

A default instance of the serializer is available as `npy_serializer`:

```python
import numpy as np

from labox.extra.numpy import npy_serializer

arr = np.array([[1, 2, 3], [4, 5, 6]])

serialized_data = npy_serializer.serialize_data(arr)
```

You can also create a custom instance:

```python
from labox.extra.numpy import NpySerializer

serializer = NpySerializer(
    load_args={"mmap_mode": "r"},
)
```

Numpy supports [pickling][pickle] native Python objects found within arrays with an
`allow_pickle` argument but this has been intentionally disabled because pickled objects
both pose a security risk and are not a stable long-term storage format.

### Content Type

The serializer uses the content type `"application/x-npy.v3"` to identify `.npy` format
data, following the NumPy file format specification version 3.0.
