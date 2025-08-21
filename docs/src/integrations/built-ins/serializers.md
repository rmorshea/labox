# Serializers

Labox provides built-in serializers for various data types.

## JSON

Both a [`JsonSerializer`][labox.builtin.serializers.JsonSerializer] and
[`JsonStreamSerializer`][labox.builtin.serializers.JsonStreamSerializer] implementations
are available. Either can be configured with an optional
[`JSONEncoder`][json.JSONEncoder] and/or [`JSONDecoder`][json.JSONDecoder].

```python
import json

from labox.builtin.serializers import JsonSerializer
from labox.builtin.serializers import json_serializer
from labox.builtin.serializers import json_stream_serializer

custom_json_serializer = JsonSerializer(
    encoder=json.JSONEncoder(indent=2),
    decoder=json.JSONDecoder(object_hook=lambda d: d),
)
```

## CSV

A [`CsvSerializer`][labox.builtin.serializers.CsvSerializer] implementation is
available. It can be configured with
[`CsvOptions`][labox.builtin.serializers.CsvOptions] that are similat to those passed to
[`csv.writer`][csv.writer] and [`csv.reader`][csv.reader]. Unlike those though, you
cannot pass a [`csv.Dialect`][csv.Dialect] directly. Instead, you may pass a dialect
name as a string. For custom dialects, you can first use
[`csv.register_dialect`][csv.register_dialect] to add it under a name you choose, and
then pass that name to the serializer.

```python
from labox.builtin.serializers import CsvSerializer
from labox.builtin.serializers import csv_serializer

unix_csv_serializer = CsvSerializer(dialect="unix")
```

## Datetime

Labox provides a basic
[`Iso8601Serializer`][labox.builtin.serializers.Iso8601Serializer] for serializing
datetime objects to ISO 8601 strings.

```python
from labox.builtin.serializers import iso8601_serializer
```
