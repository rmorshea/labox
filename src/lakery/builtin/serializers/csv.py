import csv
import json
from collections.abc import Sequence
from io import StringIO
from typing import Required
from typing import TypedDict
from typing import Unpack

from lakery.core.serializer import SerializedData
from lakery.core.serializer import Serializer


class CsvOptions(TypedDict, total=False):
    """Configuration for CSV serialization."""

    dialect: Required[csv.Dialect]
    delimiter: str
    quotechar: str | None
    escapechar: str | None
    doublequote: bool
    skipinitialspace: bool
    lineterminator: str
    quoting: int
    strict: bool


class CsvSerializer(Serializer[Sequence]):
    """Serializer for CSV format."""

    name = "lakery.csv@v1"
    types = (list, tuple)  # Assuming CSV will serialize lists or tuples of values
    content_type = "text/csv"

    def __init__(self, **options: Unpack[CsvOptions]) -> None:
        """Initialize the CSV serializer with optional format parameters."""
        self.options = options

    def serialize_data(self, value: Sequence) -> SerializedData:
        """Serialize the given value to CSV."""
        buffer = StringIO(f"#{json.dumps(self.options)}\n")
        writer = csv.writer(buffer, **self.options)
        for row in value:
            writer.writerow(row)
        return {
            "content_encoding": "utf-8",
            "content_type": self.content_type,
            "data": buffer.getvalue().encode("utf-8"),
        }

    def deserialize_data(self, content: SerializedData) -> Sequence:
        """Deserialize the given CSV data."""
        buffer = StringIO(content["data"].decode("utf-8"))
        options = json.loads(buffer.readline().lstrip("#").strip())
        reader = csv.reader(buffer, **options)
        return list(reader)
