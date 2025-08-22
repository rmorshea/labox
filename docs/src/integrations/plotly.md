# Plotly

!!! note

    Install with `pip install labox[plotly]`

## Figure Serializer

The [`FigureSerializer`][labox.extra.plotly.FigureSerializer] provides a
[serializer](../concepts/serializers.md) implementation for Plotly figures using
Plotly's native JSON format. This serializer leverages Plotly's built-in
[`plotly.io.to_json`][plotly.io.to_json] and
[`plotly.io.from_json`][plotly.io.from_json] functions to efficiently serialize and
deserialize [`plotly.graph_objects.Figure`][plotly.graph_objects.Figure] objects.

### Basic Usage

A default instance of the serializer is available as `figure_serializer`:

```python
import plotly.graph_objects as go

from labox.extra.plotly import figure_serializer

fig = go.Figure(data=go.Bar(x=["A", "B", "C"], y=[1, 3, 2]))
fig.update_layout(title="Sample Bar Chart")

serialized_data = figure_serializer.serialize_data(fig)
```

You can also create a custom instance with a more specific configuration:

```python
from labox.extra.plotly import FigureSerializer

serializer = FigureSerializer(
    dump_args={"pretty": True, "remove_uids": True},
    load_args={"skip_invalid": True},
)
```
