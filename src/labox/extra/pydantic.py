from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import AsyncIterable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import TypedDict
from typing import Unpack
from typing import cast
from typing import overload

from pydantic import BaseModel
from pydantic import ConfigDict as _ConfigDict
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema as cs
from pydantic_walk_core_schema import walk_core_schema

from labox._internal._simplify import LaboxContentDict
from labox._internal._simplify import LaboxRefDict
from labox._internal._utils import frozenclass
from labox._internal._utils import get_typed_dict
from labox.core.serializer import StreamSerializer
from labox.core.storable import Storable
from labox.core.storable import StorableConfigDict
from labox.core.unpacker import AnyUnpackedValue
from labox.core.unpacker import UnpackedValue
from labox.core.unpacker import UnpackedValueStream
from labox.core.unpacker import Unpacker

if TYPE_CHECKING:
    from labox.core.registry import Registry
    from labox.core.serializer import Serializer
    from labox.core.storage import Storage


__all__ = (
    "StorableModel",
    "StorableSpec",
)

_LOG = getLogger(__name__)


class StorableModelUnpacker(Unpacker["StorableModel"]):
    """Unpacker for [`StorableModel`][labox.extra.pydantic.StorableModel] objects."""

    name = "labox.pydantic@v1"

    def unpack_object(
        self,
        obj: StorableModel,
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
            mode="json",
            context=_make_serialization_context(
                _LaboxSerializationContext(
                    external=external,
                    get_next_external_id=get_next_external_id,
                    registry=registry,
                )
            ),
            serialize_as_any=True,
        )

        return {
            "body": {
                "value": data,
                "serializer": obj.storable_body_serializer(registry),
                "storage": obj.storable_body_storage(registry),
            },
            **external,
        }

    def repack_object(
        self,
        cls: type[StorableModel],
        contents: Mapping[str, AnyUnpackedValue],
        registry: Registry,
    ) -> StorableModel:
        """Load the model from storage content."""
        contents = dict(contents)
        try:
            unpacked_body = contents["body"]
            body_value = unpacked_body["value"]  # type: ignore[reportGeneralTypeIssues]
        except KeyError:
            msg = "Missing or malformed 'body' key in model contents."
            raise ValueError(msg) from None
        return cls.model_validate(
            body_value,
            context=_make_validation_context(
                _LaboxValidationContext(external=contents, registry=registry)
            ),
        )


class ConfigDict(StorableConfigDict, _ConfigDict):
    """Configuration for a storage model.

    See [`StorableConfigDict`][labox.core.storable.StorableConfigDict] and
    [`pydantic.ConfigDict`][pydantic.ConfigDict] for more details.
    """


class StorableModel(
    Storable,
    BaseModel,
    arbitrary_types_allowed=True,
    storable_unpacker=StorableModelUnpacker(),
):
    """A Pydantic model that can be stored by Labox."""

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
            StorableModel  # type:ignore[reportUnusedExpression] # noqa: B018
        except NameError:
            # we're defining the schema for this class
            return handler(source)
        else:
            # we're defining the schema for a subclass
            return _adapt_third_party_types(handler(source), handler)

    def storable_body_storage(self, registry: Registry) -> Storage:
        """Return the storage for the "body" of this model.

        "Body" refers to the data within the model that does not have an explicit storage.
        """
        return registry.get_default_storage()

    def storable_body_serializer(self, registry: Registry) -> Serializer:
        """Return a JSON serializer for the "body" of this model.

        "Body" refers to the data within the model that does not have an explicit storage.
        """
        return registry.get_serializer_by_content_type("application/json")


@frozenclass
class StorableSpec:
    """An annotation for specifying the storage and serialization of a value.

    Use [`typing.Annotated`][typing.Annotated] to add this to any type annotation.

    ```python
    from typing import Any, Annotated, TypeVar
    from labox.extra.pydantic import StorableSpec
    from labox.extra.msgpack import MsgPackSerializer

    msgpack_serializer = MsgPackSerializer()

    T = TypeVar("T")
    UseMsgPack = Annotated[T, StorableSpec(serializer=msgpack_serializer)]
    ```

    Then use it somewhere in a storage model:

    ```python
    from labox.extra.pydantic import StorableModel


    class MyModel(StorableModel, class_id="..."):
        my_field: UseMsgPack[Any]
    ```
    """

    serializer: type[Serializer | StreamSerializer] | None = None
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
            if issubclass(self.serializer, StreamSerializer):
                metadata["serializer_name"] = self.serializer.name  # type: ignore
                metadata["serializer_type"] = "stream_serializer"
            else:
                metadata["serializer_name"] = self.serializer.name  # type: ignore
                metadata["serializer_type"] = "serializer"

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
    def validate(maybe_labox_dict: Any, info: cs.FieldValidationInfo, /) -> Any:
        if not _has_info_context(info):
            return maybe_labox_dict

        context = _get_info_context(info)
        registry = context["registry"]

        if not isinstance(maybe_labox_dict, Mapping):
            return maybe_labox_dict

        if "__labox__" not in maybe_labox_dict:
            return maybe_labox_dict

        labox_dict = cast("LaboxContentDict | LaboxRefDict", maybe_labox_dict)

        if labox_dict["__labox__"] == "ref":
            ref_str = labox_dict["ref"]
            match context["external"][ref_str]:
                case {"value": value}:
                    return value
                case {"value_stream": value_stream}:
                    return value_stream
                case _:
                    msg = f"Invalid external content reference: {ref_str}."
                    raise ValueError(msg)
        elif labox_dict["__labox__"] == "content":
            serializer = registry.get_serializer(labox_dict["serializer_name"])
            return serializer.deserialize_data(
                {
                    "data": b64decode(labox_dict["content_base64"].encode("ascii")),
                    "content_encoding": labox_dict["content_encoding"],
                    "content_type": labox_dict["content_type"],
                }
            )
        else:  # nocov
            msg = f"Unknown Labox JSON type: {labox_dict['__labox__']}."
            raise ValueError(msg)

    return validate


def _make_serializer_func(schema: cs.CoreSchema) -> cs.FieldPlainInfoSerializerFunction:
    metadata = _get_schema_metadata(schema)
    serializer_name = metadata.get("serializer_name")
    serializer_type_ = metadata.get("serializer_type")
    storage_name = metadata.get("storage_name")

    def serialize(
        model: BaseModel,
        value: Any,
        info: cs.FieldSerializationInfo,
    ) -> LaboxContentDict | LaboxRefDict:
        context = _get_info_context(info)
        external = context["external"]
        registry = context["registry"]

        serializer_type = (
            serializer_type_
            if serializer_type_ is not None
            else ("stream_serializer" if isinstance(value, AsyncIterable) else "serializer")
        )

        if serializer_type == "serializer":
            serializer = (
                registry.get_serializer(serializer_name)
                if serializer_name is not None
                else registry.get_serializer_by_type(type(value))
            )
            if storage_name is not None:
                ref_str = _make_ref_str(type(model), info, context)
                unpacked = UnpackedValue(
                    value=value,
                    serializer=serializer,
                    storage=registry.get_storage(storage_name),
                )
                external[ref_str] = unpacked
                return {"__labox__": "ref", "ref": ref_str}
            content = serializer.serialize_data(value)
            return LaboxContentDict(
                __labox__="content",
                content_base64=b64encode(content["data"]).decode("ascii"),
                content_encoding=content["content_encoding"],
                content_type=content["content_type"],
                serializer_name=serializer.name,
            )
        else:
            serializer = (
                registry.get_stream_serializer(serializer_name)
                if serializer_name is not None
                else None
            )
            storage = (
                registry.get_storage(storage_name)
                if storage_name is not None
                else registry.get_default_storage()
            )
            unpacked = UnpackedValueStream(
                value_stream=value,
                serializer=serializer,
                storage=storage,
            )
            ref_str = _make_ref_str(type(model), info, context)
            external[ref_str] = unpacked
            return LaboxRefDict(__labox__="ref", ref=ref_str)

    return serialize


def _make_ref_str(
    model_type: type[BaseModel],
    info: cs.FieldSerializationInfo,
    context: _LaboxSerializationContext,
) -> str:
    ext_id = context["get_next_external_id"]()
    return f"ref.{model_type.__name__}.{info.field_name}.{ext_id}"


_LABOX_KEY = "labox"


def _has_schema_metadata(schema: cs.CoreSchema) -> bool:
    if not isinstance(metadata := schema.get("metadata"), Mapping):
        return False
    return _LABOX_KEY in metadata


def _get_schema_metadata(schema: cs.CoreSchema) -> _SchemaMetadata:
    if not isinstance(metadata := schema.get("metadata"), Mapping):
        return {}
    return metadata.get(_LABOX_KEY, {})


def _set_schema_metadata(schema: cs.CoreSchema, metadata: _SchemaMetadata) -> None:
    if "metadata" in schema:
        if not isinstance(existing_metadata := schema["metadata"], Mapping):
            msg = "Failed to set schema metadata - expected a mapping, got %s."
            _LOG.warning(msg, existing_metadata)
            return
    else:
        existing_metadata = schema["metadata"] = {}
    if isinstance(existing_metadata, MutableMapping):
        existing_metadata[_LABOX_KEY] = metadata
    schema["metadata"] = {_LABOX_KEY: metadata}


class _SchemaMetadata(TypedDict, total=False):
    serializer_name: str
    serializer_type: Literal["serializer", "stream_serializer"]
    storage_name: str


def _make_validation_context(ctx: _LaboxValidationContext) -> dict[str, Any]:
    return {_LABOX_KEY: ctx}


def _make_serialization_context(ctx: _LaboxSerializationContext) -> dict[str, Any]:
    return {_LABOX_KEY: ctx}


def _has_info_context(info: cs.ValidationInfo | cs.SerializationInfo) -> bool:
    return isinstance(info.context, Mapping) and _LABOX_KEY in info.context


@overload
def _get_info_context(info: cs.ValidationInfo) -> _LaboxValidationContext: ...


@overload
def _get_info_context(info: cs.SerializationInfo) -> _LaboxSerializationContext: ...


def _get_info_context(
    info: cs.ValidationInfo | cs.SerializationInfo,
) -> _LaboxValidationContext | _LaboxSerializationContext:
    if not isinstance(ctx := info.context, Mapping):
        msg = "Missing labox context."
        raise TypeError(msg)
    try:
        return ctx[_LABOX_KEY]
    except KeyError:
        msg = "Missing labox context."
        raise TypeError(msg) from None


class _LaboxSerializationContext(TypedDict):
    external: dict[str, AnyUnpackedValue]
    get_next_external_id: Callable[[], int]
    registry: Registry


class _LaboxValidationContext(TypedDict):
    external: dict[str, AnyUnpackedValue]
    registry: Registry
