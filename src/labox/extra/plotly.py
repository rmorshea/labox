from __future__ import annotations

from typing import TypedDict
from typing import cast

from plotly import graph_objects as go
from plotly.io import from_json
from plotly.io import to_json

from labox.core.serializer import SerializedData
from labox.core.serializer import Serializer

__all__ = (
    "FigureDumpArgs",
    "FigureLoadArgs",
    "FigureSerializer",
    "figure_serializer",
)


class FigureDumpArgs(TypedDict, total=False):
    """Arguments for dumping a Plotly figure."""

    engine: str | None
    """JSON encoder to use. Options include "json", "orjson", or "auto". Default is None."""
    pretty: bool
    """Whether to pretty print the JSON representation (default False)."""
    remove_uids: bool
    """Whether to remove trace UIDs from the JSON representation (default True)."""
    validate: bool
    """Whether to validate the figure before serialization (default True)."""


class FigureLoadArgs(TypedDict, total=False):
    """Arguments for loading a Plotly figure."""

    engine: str | None
    skip_invalid: bool


class FigureSerializer(Serializer[go.Figure]):
    """Serializer for Plotly figures.

    Args:
        dump_args: Arguments for dumping a Plotly figure.
        load_args: Arguments for loading a Plotly figure.
    """

    name = "labox.plotly.value@v1"
    types = (go.Figure,)
    content_type = "application/vnd.plotly.v1+json"

    def __init__(
        self,
        *,
        dump_args: FigureDumpArgs | None = None,
        load_args: FigureLoadArgs | None = None,
    ) -> None:
        self._dump_args = dump_args or {}
        self._load_args = load_args or {}

    def serialize_data(self, value: go.Figure, /) -> SerializedData:
        """Serialize the given figure."""
        data = cast("str", to_json(value, **self._dump_args))
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "data": data.encode("utf-8"),
        }

    def deserialize_data(self, content: SerializedData, /) -> go.Figure:
        """Deserialize the given figure."""
        data = content["data"].decode("utf-8")
        return from_json(data, **self._load_args)


figure_serializer = FigureSerializer()
"""FigureSerializer with default settings."""
