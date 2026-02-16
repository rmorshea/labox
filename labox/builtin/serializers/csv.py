import csv
from collections.abc import Sequence
from io import StringIO
from typing import Literal
from typing import TypedDict
from typing import Unpack

from labox.core.serializer import SerializedData
from labox.core.serializer import Serializer


class CsvOptions(TypedDict, total=False):
    """Configuration for CSV serialization."""

    dialect: str
    delimiter: str
    quotechar: str | None
    escapechar: str | None
    doublequote: bool
    skipinitialspace: bool
    lineterminator: str
    quoting: Literal[0, 1, 2, 3, 4, 5]
    strict: bool


class CsvSerializer(Serializer[Sequence, CsvOptions]):
    """Serializer for CSV format."""

    name = "labox.csv@v1"
    types = ()  # Too hard to know the data is tabular just on an object type
    content_types = ("text/csv",)

    def __init__(self, **options: Unpack[CsvOptions]) -> None:
        """Initialize the CSV serializer with optional format parameters."""
        self.options = options

    def serialize_data(self, value: Sequence) -> SerializedData[CsvOptions]:
        """Serialize the given value to CSV."""
        buffer = StringIO()
        writer = csv.writer(buffer, **self.options)
        for row in value:
            writer.writerow(row)
        return {
            "content_encoding": "utf-8",
            "content_type": "text/csv",
            "data": buffer.getvalue().encode("utf-8"),
            "config": self.options,
        }

    def deserialize_data(self, content: SerializedData[CsvOptions]) -> Sequence:
        """Deserialize the given CSV data."""
        options = content.get("config", {})
        buffer = StringIO(content["data"].decode("utf-8"))
        reader = csv.reader(buffer, **options)
        return list(reader)


csv_serializer = CsvSerializer()
""""CSV serializer with default options."""
