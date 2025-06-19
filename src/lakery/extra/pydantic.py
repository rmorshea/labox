from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import TypedDict
from typing import Unpack
from typing import cast
from typing import overload

from pydantic import BaseModel
from pydantic import ConfigDict as _ConfigDict
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema as cs
from pydantic_walk_core_schema import walk_core_schema

from lakery._internal.utils import frozenclass
from lakery._internal.utils import get_typed_dict
from lakery.core.storable import Storable
from lakery.core.storable import StorableConfigDict
from lakery.core.unpacker import AnyUnpackedValue
from lakery.core.unpacker import UnpackedValue
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from lakery.common.jsonext import AnyJsonExt
    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.storage import Storage


__all__ = (
    "StorageModel",
    "StorageSpec",
)

_LOG = getLogger(__name__)


class _StorageModelUnpacker(Unpacker["StorageModel"]):
    """Unpacker for [`StorageModel`][lakery.extra.pydantic.StorageModel] objects."""

    name = "lakery.pydantic@v1"

    def unpack_object(
        self,
        obj: StorageModel,
        registry: Registry,
    ) -> Mapping[str, AnyUnpackedValue]:
        """Dump the model to storage content."""
        external: dict[str, AnyUnpackedValue] = {}

        next_external_id = 0

        def get_next_external_id() -> int:
            nonlocal next_external_id
            next_external_id += 1
            return next_external_id

        data = obj.model_dump(
            mode="python",
            context=_make_serialization_context(
                _LakerySerializationContext(
                    external=external,
                    get_next_external_id=get_next_external_id,
                    registry=registry,
                )
            ),
            serialize_as_any=True,
        )

        return {
            "data": {
                "value": data,
                "serializer": obj.storage_model_body_serializer(registry),
                "storage": obj.storage_model_body_storage(registry),
            },
            **external,
        }

    def repack_object(
        self,
        cls: type[StorageModel],
        contents: Mapping[str, AnyUnpackedValue],
        registry: Registry,
    ) -> StorageModel:
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
                _LakeryValidationContext(external=contents, registry=registry)
            ),
        )


class ConfigDict(StorableConfigDict, _ConfigDict):
    """Configuration for a storage model.

    See [`StorableConfigDict`][lakery.core.storable.StorableConfigDict] and
    [`pydantic.ConfigDict`][pydantic.ConfigDict] for more details.
    """


class StorageModel(
    Storable,
    BaseModel,
    arbitrary_types_allowed=True,
    storable_unpacker=_StorageModelUnpacker(),
):
    """A Pydantic model that can be stored by Lakery."""

    def __init_subclass__(cls, **kwargs: Unpack[ConfigDict]) -> None:
        storable_config = get_typed_dict(StorableConfigDict, kwargs)
        pydantic_config = get_typed_dict(_ConfigDict, kwargs)
        super().__init_subclass__(**storable_config)
        super(Storable).__init_subclass__(**pydantic_config)

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

    def storage_model_body_storage(self, registry: Registry) -> Storage:
        """Return the storage for the "body" of this model.

        "Body" refers to the data that Pydantic was able to dump
        without needing to use a serializer supplied by Lakery.
        """
        return registry.get_default_storage()

    def storage_model_body_serializer(self, registry: Registry) -> Serializer:
        """Return a JSON serializer for the "body" of this model.

        "Body" refers to the data that Pydantic was able to dump
        without needing to use a serializer supplied by Lakery.
        """
        return registry.infer_serializer(dict)


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

    serializer: type[Serializer] | None = None
    """The serializer to use for this value."""
    storage: type[Storage] | None = None
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
            metadata["serializer_name"] = self.serializer.name
        if self.storage is not None:
            metadata["storage_name"] = self.storage.name
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
        registry = context["registry"]

        if not isinstance(maybe_json_ext, Mapping):
            return maybe_json_ext

        if "__json_ext__" not in maybe_json_ext:
            return maybe_json_ext

        json_ext = cast("AnyJsonExt", maybe_json_ext)

        if json_ext["__json_ext__"] == "ref":
            ref_str = json_ext["ref"]
            match context["external"][ref_str]:
                case {"value": value}:
                    return value
                case {"value_stream": value_stream}:
                    return value_stream
                case _:
                    msg = f"Invalid external content reference: {ref_str}."
                    raise ValueError(msg)
        elif json_ext["__json_ext__"] == "content":
            serializer = registry.get_serializer(json_ext["serializer_name"])
            return serializer.deserialize_data(
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
    serializer_name = metadata.get("serializer_name")
    storage_name = metadata.get("storage_name")

    def serialize(model: BaseModel, value: Any, info: cs.FieldSerializationInfo, /) -> AnyJsonExt:
        context = _get_info_context(info)
        external = context["external"]
        registry = context["registry"]

        cls = type(value)
        serializer = (
            registry.get_serializer(serializer_name)
            if serializer_name is not None
            else registry.infer_serializer(cls)
        )
        if storage_name is not None:
            ref_str = _make_ref_str(type(model), info, context)
            external[ref_str] = UnpackedValue(
                value=value,
                serializer=serializer,
                storage=registry.get_storage(storage_name),
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
    serializer_name: str
    storage_name: str


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
    external: dict[str, AnyUnpackedValue]
    get_next_external_id: Callable[[], int]
    registry: Registry


class _LakeryValidationContext(TypedDict):
    external: dict[str, AnyUnpackedValue]
    registry: Registry
