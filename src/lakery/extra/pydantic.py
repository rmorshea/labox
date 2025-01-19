from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import LiteralString
from typing import Self
from typing import TypedDict
from typing import Unpack
from typing import cast
from typing import overload
from uuid import UUID
from uuid import uuid4
from warnings import warn

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import GetCoreSchemaHandler
from pydantic._internal._core_utils import walk_core_schema as _walk_core_schema
from pydantic_core import core_schema as cs

from lakery.core.model import AnyValueDump
from lakery.core.model import BaseStorageModel
from lakery.core.model import ModelRegistry
from lakery.core.model import ValueDump
from lakery.core.serializer import Serializer
from lakery.core.storage import Storage

if TYPE_CHECKING:
    from lakery.core.context import Registries
    from lakery.core.serializer import SerializerRegistry
    from lakery.core.storage import StorageRegistry
    from lakery.extra.lakery.json_ext import AnyJsonExt


_LOG = getLogger(__name__)
_MODELS: set[type[StorageModel]] = set()


def get_model_registry() -> ModelRegistry:
    """Return a registry of all currently defined Pydantic storage models."""
    return ModelRegistry(list(_MODELS))


class StorageModel(
    BaseModel,
    BaseStorageModel[Mapping[str, AnyValueDump]],
    arbitrary_types_allowed=True,
):
    """A Pydantic model that can be stored by Lakery."""

    def __init_subclass__(
        cls,
        storage_id: LiteralString | None,
        **kwargs: Unpack[ConfigDict],
    ) -> None:
        if (super_init_subclass := super().__init_subclass__) is not object.__init_subclass__:
            super_init_subclass(**kwargs)

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

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> cs.CoreSchema:
        try:
            StorageModel  # type:ignore[reportUnusedExpression] # noqa: B018
        except NameError:
            # we're defining the schema for this class
            return handler(source)
        else:
            # we're defining the schema for a subclass
            return _adapt_third_party_types(handler(source), handler)

    def storage_model_dump(self, registries: Registries) -> Mapping[str, AnyValueDump]:
        """Turn the given model into its serialized components."""
        external_content: dict[str, AnyValueDump] = {}

        next_external_id = 0

        def get_external_id() -> int:
            nonlocal next_external_id
            next_external_id += 1
            return next_external_id

        data = self.model_dump(
            mode="python",
            context=_make_serialization_context(
                _LakerySerializationContext(
                    external_content=external_content,
                    get_external_id=get_external_id,
                    registries=registries,
                )
            ),
        )

        return {
            "data": {
                "value": data,
                "serializer": self.storage_model_internal_serializer(registries.serializers),
                "storage": self.storage_model_internal_storage(registries.storages),
            },
            **external_content,
        }

    @classmethod
    def storage_model_load(
        cls,
        spec: Mapping[str, AnyValueDump],
        registries: Registries,
    ) -> Self:
        """Turn the given serialized components back into a model."""
        spec_dict = dict(spec)
        data = cast("ValueDump", spec_dict.pop("data"))["value"]
        return cls.model_validate(
            data,
            context=_make_validation_context(
                _LakeryValidationContext(
                    external_content=spec_dict,
                    registries=registries,
                )
            ),
        )

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


@dataclass(frozen=True)
class StorageSpecMetadata:
    """An annotation for specifying the storage and serialization of a field.

    This could be used directly via `Annotated[..., StorageSpecMetadata(...)]` but
    for convenience a shorthand `StorageSpec[...]` is provided. When using the
    shorthand positional arguments can be passed with the syntax:

    ```python
    StorageSpec[my_serializer, my_storage]
    ```

    Keyword arguments can be provides with the slice syntax:

    ```python
    StorageSpec["serializer":my_serializer, "storage":my_storage]
    ```
    """

    serializer: Serializer | None
    storage: Storage | None

    def __post_init__(self) -> None:
        if self.serializer is not None and not isinstance(self.serializer, Serializer):
            msg = f"Expected a Serializer, got {self.serializer}."
            raise TypeError(msg)
        if self.storage is not None and not isinstance(self.storage, Storage):
            msg = f"Expected a Storage, got {self.storage}."
            raise TypeError(msg)

    def __class_getitem__(cls, args: tuple) -> Annotated:
        annotation, *metadata = args
        serializer: Serializer | None = None
        storage: Storage | None = None
        for m in metadata:
            match m:
                case Serializer():
                    serializer = m
                case Storage():
                    storage = m
                case _:
                    msg = f"Expected a Serializer or Storage, got {m!r}."
                    raise TypeError(msg)
        return Annotated[annotation, StorageSpecMetadata(serializer, storage)]

    def __get_pydantic_core_schema__(
        self,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> cs.CoreSchema:
        schema = handler(source)
        metadata: _SchemaMetadata = {}
        if self.serializer is not None:
            metadata["serializer"] = self.serializer
        if self.storage is not None:
            metadata["storage"] = self.storage
        _set_schema_metadata(schema, metadata)
        return schema


if TYPE_CHECKING:
    StorageSpec = Annotated
    """A type-checker friendly alias for `StorageSpecMetadata`"""
else:
    StorageSpec = StorageSpecMetadata


def _adapt_third_party_types(schema: cs.CoreSchema, handler: GetCoreSchemaHandler) -> cs.CoreSchema:
    def visit_is_instance_schema(schema: cs.CoreSchema, recurse):
        if schema["type"] == "definition-ref":
            return recurse(handler.resolve_ref_schema(schema), visit_is_instance_schema)
        elif schema["type"] in ("is-instance", "any"):
            return _adapt_core_schema(schema)
        elif "serialization" in schema:
            return schema  # Already adapted
        else:
            return recurse(schema, visit_is_instance_schema)

    return _walk_core_schema(schema, visit_is_instance_schema)


def _adapt_core_schema(schema: cs.CoreSchema) -> cs.JsonOrPythonSchema:
    return cs.json_or_python_schema(
        json_schema=cs.any_schema(),
        python_schema=cs.chain_schema(
            [
                cs.with_info_plain_validator_function(_make_validator_func()),
                schema,
            ]
        ),
        serialization={
            "type": "function-plain",
            "function": _make_serializer_func(schema),
            "is_field_serializer": True,
            "info_arg": True,
            "return_schema": cs.any_schema(),
            "when_used": "always",
        },
    )


def _make_validator_func() -> cs.WithInfoValidatorFunction:
    def validate(maybe_json_ext: Any, info: cs.FieldValidationInfo, /) -> Any:
        if not _has_info_context(info):
            return maybe_json_ext

        context = _get_info_context(info)
        registries = context["registries"]

        if not isinstance(maybe_json_ext, Mapping):
            return maybe_json_ext

        if "__json_ext__" not in maybe_json_ext:
            return maybe_json_ext

        json_ext = cast("AnyJsonExt", maybe_json_ext)

        if json_ext["__json_ext__"] == "ref":
            ref_str = json_ext["ref"]
            spec = context["external_content"][ref_str]
            if "value" in spec:
                return spec["value"]
            elif "stream" in spec:
                return spec["stream"]
            else:  # nocov
                msg = f"Invalid external content reference: {ref_str}."
                raise ValueError(msg)
        elif json_ext["__json_ext__"] == "content":
            serializer = registries.serializers[json_ext["serializer_name"]]
            return serializer.load(
                {
                    "content": b64decode(json_ext["content_base64"].encode("ascii")),
                    "content_encoding": json_ext["content_encoding"],
                    "content_type": json_ext["content_type"],
                }
            )
        else:  # nocov
            msg = f"Unknown JSON extension type: {json_ext['__json_ext__']}."
            raise ValueError(msg)

    return validate


def _make_serializer_func(schema: cs.CoreSchema) -> cs.FieldPlainInfoSerializerFunction:
    metadata = _get_schema_metadata(schema)
    serializer_from_schema = metadata.get("serializer")
    storage_from_schema = metadata.get("storage")

    def serialize(model: BaseModel, value: Any, info: cs.FieldSerializationInfo, /) -> Any:
        context = _get_info_context(info)
        external_content = context["external_content"]
        registries = context["registries"]

        cls = type(value)
        serializer = serializer_from_schema or registries.serializers.infer_from_value_type(cls)

        if storage_from_schema is not None:
            ref_str = _make_ref_str(type(model), info, context)
            external_content[ref_str] = ValueDump(
                value=value,
                serializer=serializer,
                storage=storage_from_schema,
            )
            return {"__json_ext__": "ref", "ref": ref_str}

        dump = serializer.dump(value)
        return {
            "__json_ext__": "content",
            "content_base64": b64encode(dump["content"]).decode("ascii"),
            "content_encoding": None,
            "content_type": dump["content_type"],
            "serializer_name": serializer.name,
            "serializer_version": serializer.version,
        }

    return serialize


def _make_ref_str(
    model_type: type[BaseModel],
    info: cs.FieldSerializationInfo,
    context: _LakerySerializationContext,
) -> str:
    return f"ref.{model_type.__name__}.{info.field_name}.{context['get_external_id']()}"


_LAKERY_KEY = "lakery"


def _get_schema_metadata(schema: cs.CoreSchema) -> _SchemaMetadata:
    if not isinstance(metadata := schema.get("metadata"), Mapping):
        return {}
    return metadata.get(_LAKERY_KEY, {})


def _set_schema_metadata(schema: cs.CoreSchema, metadata: _SchemaMetadata) -> None:
    if "metadata" in schema:
        if not isinstance(existing_metadata := schema["metadata"], Mapping):
            msg = "Failed to set schema metadata - expected a mapping, got %s."
            _LOG.warning(msg, existing_metadata)
            return
    else:
        existing_metadata = schema["metadata"] = {}
    if isinstance(existing_metadata, MutableMapping):
        existing_metadata[_LAKERY_KEY] = metadata
    schema["metadata"] = {_LAKERY_KEY: metadata}


class _SchemaMetadata(TypedDict, total=False):
    serializer: Serializer
    storage: Storage


def _make_validation_context(ctx: _LakeryValidationContext) -> dict[str, Any]:
    return {_LAKERY_KEY: ctx}


def _make_serialization_context(ctx: _LakerySerializationContext) -> dict[str, Any]:
    return {_LAKERY_KEY: ctx}


def _has_info_context(info: cs.ValidationInfo | cs.SerializationInfo) -> bool:
    return isinstance(info.context, Mapping) and _LAKERY_KEY in info.context


@overload
def _get_info_context(info: cs.ValidationInfo) -> _LakeryValidationContext: ...


@overload
def _get_info_context(info: cs.SerializationInfo) -> _LakerySerializationContext: ...


def _get_info_context(
    info: cs.ValidationInfo | cs.SerializationInfo,
) -> _LakeryValidationContext | _LakerySerializationContext:
    if not isinstance(ctx := info.context, Mapping):
        msg = "Missing lakery context."
        raise TypeError(msg)
    try:
        return ctx[_LAKERY_KEY]
    except KeyError:
        msg = "Missing lakery context."
        raise TypeError(msg) from None


class _LakerySerializationContext(TypedDict):
    external_content: dict[str, AnyValueDump]
    get_external_id: Callable[[], int]
    registries: Registries


class _LakeryValidationContext(TypedDict):
    external_content: dict[str, AnyValueDump]
    registries: Registries
