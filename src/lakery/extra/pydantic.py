from __future__ import annotations

from collections.abc import AsyncIterable
from collections.abc import Callable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import Self
from typing import TypedDict
from typing import cast
from typing import overload

from pydantic import BaseModel
from pydantic import GetCoreSchemaHandler
from pydantic._internal._core_utils import walk_core_schema as _walk_core_schema
from pydantic_core import core_schema as cs

from lakery.core.model import StorageModel as _StorageModel
from lakery.core.model import StorageSpec
from lakery.core.model import StreamStorageSpec
from lakery.core.model import ValueStorageSpec
from lakery.core.serializer import StreamSerializer

if TYPE_CHECKING:
    from lakery.core.context import Registries
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import Storage


class StorageModel(
    _StorageModel[Mapping[str, StorageSpec]],
    BaseModel,
    arbitrary_types_allowed=True,
):
    """A Pydantic model that can be stored by Lakery."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> cs.CoreSchema:
        return _adapt_third_party_types(handler(source), handler)

    def storage_model_to_spec(self, registries: Registries) -> Mapping[str, StorageSpec]:
        """Turn the given model into its serialized components."""
        external_content: dict[str, StorageSpec] = {}

        next_external_id = 0

        def get_external_id() -> int:
            nonlocal next_external_id
            next_external_id += 1
            return next_external_id

        data = self.model_dump(
            mode="python",
            context=_LakerySerializationContext(
                external_content=external_content,
                get_external_id=get_external_id,
                registries=registries,
            ),
        )

        return {"data": {"value": data, "serializer": None, "storage": None}, **external_content}

    @classmethod
    def storage_model_from_spec(
        cls,
        spec: Mapping[str, StorageSpec],
        registries: Registries,
    ) -> Self:
        """Turn the given serialized components back into a model."""
        spec_dict = dict(spec)
        data = cast("ValueStorageSpec", spec_dict.pop("data"))["value"]
        return cls.model_validate(
            data,
            context=_LakeryValidationContext(
                external_content=spec_dict,
                registries=registries,
            ),
        )


def _adapt_third_party_types(schema: cs.CoreSchema, handler: GetCoreSchemaHandler) -> cs.CoreSchema:
    def visit_is_instance_schema(schema: cs.CoreSchema, recurse):
        if schema["type"] == "definition-ref":
            return recurse(handler.resolve_ref_schema(schema), visit_is_instance_schema)
        elif schema["type"] == "is-instance":
            return _adapt_is_instance_schema(schema)
        elif "serialization" in schema:
            return schema  # Already adapted
        else:
            return recurse(schema, visit_is_instance_schema)

    return _walk_core_schema(schema, visit_is_instance_schema)


def _adapt_is_instance_schema(schema: cs.IsInstanceSchema) -> cs.JsonOrPythonSchema:
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
        context = _get_lakery_info_context(info)
        registries = context["registries"]

        if not isinstance(maybe_json_ext, Mapping):
            return maybe_json_ext

        if "__json_ext__" not in maybe_json_ext:
            return maybe_json_ext

        json_ext = cast("_JsonExt", maybe_json_ext)

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
            return serializer.load_value(
                {
                    "content_bytes": json_ext["content_base64"].encode("ascii"),
                    "content_encoding": json_ext["content_encoding"],
                    "content_type": json_ext["content_type"],
                    "serializer_name": json_ext["serializer_name"],
                    "serializer_version": json_ext["serializer_version"],
                }
            )
        else:  # nocov
            msg = f"Unknown JSON extension type: {json_ext['__json_ext__']}."
            raise ValueError(msg)

    return validate


def _make_serializer_func(
    schema: cs.IsInstanceSchema,
) -> cs.FieldPlainInfoSerializerFunction:
    metadata = _get_lakery_schema_metadata(schema)
    serializer_from_schema = metadata.get("serializer")
    storage_from_schema = metadata.get("storage")

    def serialize(model: BaseModel, value: Any, info: cs.FieldSerializationInfo, /) -> Any:
        context = _get_lakery_info_context(info)
        external_content = context["external_content"]
        registries = context["registries"]

        if isinstance(value, AsyncIterable):
            if serializer_from_schema and not isinstance(serializer_from_schema, StreamSerializer):
                msg = (
                    f"{info.field_name} of {type(model)} expects "
                    f"a StreamSerializer, got {serializer_from_schema}."
                )
                raise TypeError(msg)
            ref_str = _make_ref_str(type(model), info, context)
            external_content[ref_str] = StreamStorageSpec(
                stream=value,
                serializer=serializer_from_schema,
                storage=storage_from_schema,
            )
            return {"__json_ext__": "ref", "ref": ref_str}

        cls = type(value)
        serializer = serializer_from_schema or registries.serializers.infer_from_value_type(cls)

        if storage_from_schema is not None:
            ref_str = _make_ref_str(type(model), info, context)
            external_content[ref_str] = ValueStorageSpec(
                value=value,
                serializer=serializer,
                storage=storage_from_schema,
            )
            return {"__json_ext__": "ref", "ref": ref_str}

        dump = serializer.dump_value(value)
        return {
            "__json_ext__": "content",
            "content_base64": dump["content_bytes"].decode("ascii"),
            "content_encoding": None,
            "content_type": dump["content_type"],
            "serializer_name": dump["serializer_name"],
            "serializer_version": dump["serializer_version"],
        }

    return serialize


def _make_ref_str(
    model_type: type[BaseModel],
    info: cs.FieldSerializationInfo,
    context: _LakerySerializationContext,
) -> str:
    return f"ref.{model_type.__name__}.{info.field_name}.{context['get_external_id']()}"


_LAKERY_KEY = "lakery"


def _get_lakery_schema_metadata(schema: cs.CoreSchema) -> _LakerySchemaMetadata:
    if not isinstance(metadata := schema.get("metadata"), Mapping):
        return {}
    return metadata.get(_LAKERY_KEY, {})


class _LakerySchemaMetadata(TypedDict, total=False):
    serializer: ValueSerializer | StreamSerializer
    storage: Storage


@overload
def _get_lakery_info_context(info: cs.ValidationInfo) -> _LakeryValidationContext: ...


@overload
def _get_lakery_info_context(info: cs.SerializationInfo) -> _LakerySerializationContext: ...


def _get_lakery_info_context(
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
    external_content: dict[str, StorageSpec]
    get_external_id: Callable[[], int]
    registries: Registries


class _LakeryValidationContext(TypedDict):
    external_content: dict[str, StorageSpec]
    registries: Registries


class _JsonRefExt(TypedDict):
    __json_ext__: Literal["ref"]

    ref: str
    """The reference to the external content."""


class _JsonContentExt(TypedDict):
    __json_ext__: Literal["content"]

    content_base64: str
    """The base64 encoded data."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""
    serializer_version: int
    """The version of the serializer used to serialize the data."""


_JsonExt = _JsonRefExt | _JsonContentExt
"""The JSON extension for external content references."""
