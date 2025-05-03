from __future__ import annotations

from dataclasses import KW_ONLY
from dataclasses import Field
from dataclasses import dataclass
from dataclasses import fields
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import LiteralString
from typing import Self
from typing import TypedDict
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn

from lakery.common.jsonext import dump_any_json_ext
from lakery.common.jsonext import load_json_ext
from lakery.core.model import BaseStorageModel
from lakery.core.model import Manifest
from lakery.core.model import ManifestMap
from lakery.core.serializer import Serializer
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import Storage
from lakery.core.storage import StorageRegistry

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lakery.core.context import Registries

__all__ = ("DataclassModel",)

T = TypeVar("T")


@dataclass
class DataclassModel(BaseStorageModel):
    """A dataclass model that can be stored by Lakery."""

    _: KW_ONLY

    def __init_subclass__(
        cls,
        storage_id: LiteralString | None,
    ) -> None:
        if storage_id is None:  # nocov
            _LOG.debug("Skipping storage model registration for %s.", cls)
        else:
            try:
                UUID(storage_id)
            except ValueError:
                suggested_uuid = uuid4().hex
                full_class_name = f"{cls.__module__}.{cls.__qualname__}"
                msg = (
                    f"Storage model {full_class_name!r} cannot be stored because {storage_id=!r} "
                    f"is not a UUID - use {suggested_uuid!r} instead."
                )
                warn(msg, UserWarning, stacklevel=2)
            else:
                cls.storage_model_id = storage_id

    def storage_model_dump(self, registries: Registries) -> Mapping[str, Manifest]:
        """Dump the model into a dictionary of values."""
        external: dict[str, Manifest] = {}
        context: _DumpContext = {
            "path": "",
            "registries": registries,
            "external": external,
        }
        kwargs: dict[str, Any] = {}
        for f in fields(self):
            if not f.init:
                continue

            value = getattr(self, f.name)

            if isinstance(value, BaseStorageModel):
                msg = (
                    f"DataclassModel does not support nested models: {f.name}={value} - "
                    "consider using lakery.extra.pydantic.StorageModel instead"
                )
                raise TypeError(msg)

            data = Manifest(
                value=value,
                serializer=_get_field_serializer(f),
                storage=_get_field_storage(f),
            )

            kwargs[f.name] = dump_any_json_ext(f.name, data, context)

        return {
            "data": {
                "value": kwargs,
                "serializer": self.storage_model_internal_serializer(
                    registries.serializers
                ),
                "storage": self.storage_model_internal_storage(registries.storages),
            },
            **external,
        }

    @classmethod
    def storage_model_load(cls, manifests: ManifestMap, registries: Registries) -> Self:
        """Load the model from a dictionary a series of manifests."""
        external = cast("dict[str, Manifest]", dict(manifests))
        data = external.pop("data")["value"]
        kwargs = load_json_ext(data, {"external": external, "registries": registries})
        return cls(**kwargs)

    def storage_model_internal_storage(self, storages: StorageRegistry) -> Storage:
        """Return the storage for "internal data" for this model.

        "Internal data" refers to the data that Pydantic was able to
        dump without needing to use a serializer supplied by Lakery.
        """
        return storages.default

    def storage_model_internal_serializer(
        self, serializers: SerializerRegistry
    ) -> Serializer:
        """Return the serializer for "internal data" friom this model.

        "Internal data" refers to the data that Pydantic was able to
        dump without needing to use a serializer supplied by Lakery.
        In short this method should return a JSON serializer.
        """
        return serializers.infer_from_value_type(dict)


def _get_field_serializer(field: Field) -> Serializer | None:
    return s if isinstance(s := field.metadata.get("serializer"), Serializer) else None


def _get_field_storage(field: Field) -> Storage | None:
    return s if isinstance(s := field.metadata.get("storage"), Storage) else None


class _DumpContext(TypedDict):
    path: str
    registries: Registries
    external: dict[str, Manifest]


class _LoadContext(TypedDict):
    registries: Registries
    external: dict[str, Manifest]


_LOG = getLogger(__name__)
