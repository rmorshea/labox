from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TypeAlias

import pytest

from labox.core.registry import Registry
from labox.core.storable import Storable
from labox.core.unpacker import AnyUnpackedValue

Case: TypeAlias = tuple[str, Storable, Mapping[str, AnyUnpackedValue]]


def make_storable_unpack_repack_test(
    cases: Sequence[Case],
    registry: Registry,
) -> Callable:
    case_ids: list[str] = []
    obj_and_expected_contents: list[tuple[Storable, Mapping[str, AnyUnpackedValue]]] = []
    for id_str, obj, expected_contents in cases:
        case_ids.append(id_str)
        obj_and_expected_contents.append((obj, expected_contents))

    @pytest.mark.parametrize(("obj", "expected_contents"), obj_and_expected_contents, ids=case_ids)
    def test_case(obj: Storable, expected_contents: Mapping[str, AnyUnpackedValue]) -> None:
        unpacker = obj.storable_config().unpacker
        contents = unpacker.unpack_object(obj, registry)
        assert contents == expected_contents
        repacked_obj = unpacker.repack_object(type(obj), contents, registry)
        assert obj == repacked_obj

    return test_case
