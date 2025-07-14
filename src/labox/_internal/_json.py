import json

JSON_TYPES = (int, str, float, bool, type(None), dict, list, tuple)
"""The types that can be serialized to JSON."""
DEFAULT_JSON_ENCODER = json.JSONEncoder(separators=(",", ":"), allow_nan=False)
"""The default JSON encoder used for serialization."""
DEFAULT_JSON_DECODER = json.JSONDecoder()
"""The default JSON decoder used for deserialization."""
