from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import TypedDict
from typing import Unpack
from typing import cast
from typing import overload

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema as cs
from pydantic_walk_core_schema import walk_core_schema

from lakery._internal.utils import frozenclass
from lakery.core.unpacker import AnyModeledValue
from lakery.core.unpacker import AnyModeledValueMap
from lakery.core.unpacker import BaseStorageModel
from lakery.core.unpacker import StorageModelConfig
from lakery.core.unpacker import UnpackedValue

if TYPE_CHECKING:
    from lakery.common.jsonext import AnyJsonExt
    from lakery.core.registry import RegistryCollection
    from lakery.core.registry import SerializerRegistry
    from lakery.core.registry import StorageRegistry
    from lakery.core.serializer import Serializer
    from lakery.core.storage import Storage


__all__ = (
    "StorageModel",
    "StorageSpec",
)

_LOG = getLogger(__name__)


class StorageModel(
    BaseModel,
    BaseStorageModel[AnyModeledValueMap],
    storage_model_config=None,
    arbitrary_types_allowed=True,
):
    """A Pydantic model that can be stored by Lakery."""

    if TYPE_CHECKING:

        def __init_subclass__(
            cls,
            *,
            storage_model_config: StorageModelConfig | None,
            **kwargs: Unpack[ConfigDict],
        ) -> None: ...

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

    def storage_model_dump(self, registries: RegistryCollection) -> AnyModeledValueMap:
        """Dump the model to storage content."""
        external: dict[str, AnyModeledValue] = {}

        next_external_id = 0

        def get_next_external_id() -> int:
            nonlocal next_external_id
            next_external_id += 1
            return next_external_id

        data = self.model_dump(
            mode="python",
            context=_make_serialization_context(
                _LakerySerializationContext(
                    external=external,
                    get_next_external_id=get_next_external_id,
                    registries=registries,
                )
            ),
            serialize_as_any=True,
        )

        return {
            "data": {
                "value": data,
                "serializer": self.storage_model_internal_serializer(registries.serializers),
                "storage": self.storage_model_internal_storage(registries.storages),
            },
            **external,
        }

    @classmethod
    def storage_model_load(
        cls, contents: AnyModeledValueMap, _version: int, registries: RegistryCollection
    ) -> Self:
        """Load the model from storage content."""
        contents = dict(contents)
        try:
            data_content = contents["data"]
            data = data_content["value"]  # type: ignore[reportGeneralTypeIssues]
        except KeyError:
            msg = "Missing or malformed 'data' key in model contents."
            raise ValueError(msg) from None
        return cls.model_validate(
            data,
            context=_make_validation_context(
                _LakeryValidationContext(external=contents, registries=registries)
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


@frozenclass
class StorageSpec:
    """An annotation for specifying the storage and serialization of a field.

    Use [`typing.Annotated`][typing.Annotated] to add this to any type annotation.

    ```python
    from typing import Any, Annotated, TypeVar
    from lakery.extra.pydantic import StorageSpec
    from lakery.extra.msgpack import MsgPackSerializer

    msgpack_serializer = MsgPackSerializer()

    T = TypeVar("T")
    UseMsgPack = Annotated[T, StorageSpec(serializer=msgpack_serializer)]
    ```

    Then use it somewhere in a storage model:

    ```python
    from lakery.extra.pydantic import StorageModel


    class MyModel(StorageModel, storage_model_config={"id": "...", "version": 1}):
        my_field: UseMsgPack[Any]
    ```
    """

    serializer: Serializer | None = None
    """The serializer to use for this value."""
    storage: Storage | None = None
    """The storage to use for this value."""

    def __get_pydantic_core_schema__(
        self,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> cs.CoreSchema:
        schema = handler(source)
        if _has_schema_metadata(schema):
            metadata = _get_schema_metadata(schema)
        else:
            metadata: _SchemaMetadata = {}
        if self.serializer is not None:
            metadata["serializer"] = self.serializer
        if self.storage is not None:
            metadata["storage"] = self.storage
        _set_schema_metadata(schema, metadata)
        return schema


def _adapt_third_party_types(schema: cs.CoreSchema, handler: GetCoreSchemaHandler) -> cs.CoreSchema:
    def visit_is_instance_schema(schema: cs.CoreSchema, recurse):
        if schema["type"] == "definition-ref":
            return recurse(handler.resolve_ref_schema(schema), visit_is_instance_schema)
        elif _has_schema_metadata(schema) or schema["type"] in ("is-instance", "any"):
            return _adapt_core_schema(schema)
        elif "serialization" in schema:
            return schema  # Already adapted
        else:
            return recurse(schema, visit_is_instance_schema)

    return walk_core_schema(schema, visit_is_instance_schema)


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
            spec = context["external"][ref_str]
            if "value" in spec:
                return spec["value"]
            elif "value_stream" in spec:
                return spec["value_stream"]
            else:  # nocov
                msg = f"Invalid external content reference: {ref_str}."
                raise ValueError(msg)
        elif json_ext["__json_ext__"] == "content":
            serializer = registries.serializers[json_ext["serializer_name"]]
            return serializer.load_data(
                {
                    "data": b64decode(json_ext["content_base64"].encode("ascii")),
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
        external = context["external"]
        registries = context["registries"]

        cls = type(value)
        serializer = serializer_from_schema or registries.serializers.infer_from_value_type(cls)

        if storage_from_schema is not None:
            ref_str = _make_ref_str(type(model), info, context)
            external[ref_str] = UnpackedValue(
                value=value,
                serializer=serializer,
                storage=storage_from_schema,
            )
            return {"__json_ext__": "ref", "ref": ref_str}

        content = serializer.serialize_data(value)
        return {
            "__json_ext__": "content",
            "content_base64": b64encode(content["data"]).decode("ascii"),
            "content_encoding": None,
            "content_type": content["content_type"],
            "serializer_name": serializer.name,
        }

    return serialize


def _make_ref_str(
    model_type: type[BaseModel],
    info: cs.FieldSerializationInfo,
    context: _LakerySerializationContext,
) -> str:
    ext_id = context["get_next_external_id"]()
    return f"ref.{model_type.__name__}.{info.field_name}.{ext_id}"


_LAKERY_KEY = "lakery"


def _has_schema_metadata(schema: cs.CoreSchema) -> bool:
    if not isinstance(metadata := schema.get("metadata"), Mapping):
        return False
    return _LAKERY_KEY in metadata


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
    external: dict[str, AnyModeledValue]
    get_next_external_id: Callable[[], int]
    registries: RegistryCollection


class _LakeryValidationContext(TypedDict):
    external: dict[str, AnyModeledValue]
    registries: RegistryCollection
