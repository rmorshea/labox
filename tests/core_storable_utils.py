from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence

import pytest

from labox.core.registry import Registry
from labox.core.storable import Storable
from labox.core.unpacker import AnyUnpackedValue


def make_storable_unpack_repack_test(
    cases: Sequence[tuple[Storable, Mapping[str, AnyUnpackedValue]]],
    registry: Registry,
) -> Callable:
    @pytest.mark.parametrize(("obj", "expected_contents"), cases)
    def test_case(obj: Storable, expected_contents: Mapping[str, AnyUnpackedValue]) -> None:
        unpacker = obj.storable_config().unpacker
        contents = unpacker.unpack_object(obj, registry)
        assert contents == expected_contents
        repacked_obj = unpacker.repack_object(type(obj), contents, registry)
        assert obj == repacked_obj

    return test_case
