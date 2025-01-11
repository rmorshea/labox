from __future__ import annotations

from typing import TypedDict
from typing import cast

from plotly import graph_objects as go
from plotly.io import from_json
from plotly.io import to_json

from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

_ = to_json, from_json


class FigureDumpArgs(TypedDict, total=False):
    """Arguments for dumping a Plotly figure."""

    engine: str | None
    pretty: bool
    remove_uids: bool
    validate: bool


class FigureLoadArgs(TypedDict, total=False):
    """Arguments for loading a Plotly figure."""

    engine: str | None
    skip_invalid: bool


class FigureSerializer(ValueSerializer[go.Figure]):
    """Serializer for Plotly figures."""

    name = "lakery.plotly.value"
    version = 1
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

    def dump_value(self, value: go.Figure, /) -> ValueDump:
        """Serialize the given figure."""
        data = cast("str", to_json(value, **self._dump_args))
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "content_bytes": data.encode("utf-8"),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump, /) -> go.Figure:
        """Deserialize the given figure."""
        data = dump["content_bytes"].decode("utf-8")
        return from_json(data, **self._load_args)
