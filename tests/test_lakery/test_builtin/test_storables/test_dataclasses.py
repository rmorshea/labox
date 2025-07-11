from dataclasses import dataclass

from lakery.builtin.storables.dataclasses import StorableClass


@dataclass
class MyClass(StorableClass, class_id="..."):
    """A simple dataclass that is storable."""

    no_spec: str
    spec_with_serializer: str
    spec_with_storage: str
    spec_with_serializer_and_storage: str
