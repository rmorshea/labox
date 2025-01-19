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
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn

from lakery.core.model import BaseStorageModel
from lakery.core.model import ModelDump
from lakery.core.model import ModelRegistry
from lakery.core.model import ValueDump
from lakery.core.serializer import Serializer
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import Storage
from lakery.core.storage import StorageRegistry
from lakery.extra.lakery.json_ext import dump_any_json_ext
from lakery.extra.lakery.json_ext import load_json_ext

if TYPE_CHECKING:
    from lakery.core.context import Registries

_LOG = getLogger(__name__)
_MODELS: set[type[StorageModel]] = set()


def get_model_registry() -> ModelRegistry:
    """Return a registry of all currently defined Pydantic storage models."""
    return ModelRegistry(list(_MODELS))


@dataclass
class StorageModel(BaseStorageModel):
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
                _MODELS.add(cls)

    def storage_model_dump(self, registries: Registries) -> dict[str, ValueDump]:
        """Dump the model into a dictionary of values."""
        external: dict[str, ValueDump] = {}
        context: _DumpContext = {"path": "", "registries": registries, "external": external}
        kwargs: dict[str, Any] = {}
        for f in fields(self):
            if not f.init:
                continue

            value = getattr(self, f.name)

            data = (
                value.storage_model_dump(registries)
                if isinstance(value, BaseStorageModel)
                else ValueDump(
                    value=value,
                    serializer=_get_field_serializer(f),
                    storage=_get_field_storage(f),
                )
            )

            kwargs[f.name] = dump_any_json_ext(f.name, data, context)

        return {
            "data": {
                "value": kwargs,
                "serializer": self.storage_model_internal_serializer(registries.serializers),
                "storage": self.storage_model_internal_storage(registries.storages),
            },
            **external,
        }

    @classmethod
    def storage_model_load(cls, dump: ModelDump, registries: Registries) -> Self:
        """Load the model from a dictionary of values."""
        external = cast("dict[str, ValueDump]", dict(dump))
        data = external.pop("data")["value"]
        kwargs = load_json_ext(data, {"external": external, "registries": registries})
        return cls(**kwargs)

    def storage_model_internal_storage(self, storages: StorageRegistry) -> Storage:
        """Return the storage for "internal data" for this model.

        "Internal data" refers to the data that Pydantic was able to
        dump without needing to use a serializer supplied by Lakery.
        """
        return storages.default

    def storage_model_internal_serializer(self, serializers: SerializerRegistry) -> Serializer:
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
    external: dict[str, ValueDump]


class _LoadContext(TypedDict):
    registries: Registries
    external: dict[str, ValueDump]
