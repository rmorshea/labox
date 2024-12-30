from __future__ import annotations

import io
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import ExitStack
from typing import Any
from typing import Literal
from typing import TypedDict

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from lakery.core.serializer import StreamDump
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer


class _ArrowTableBase:
    content_type = "application/vnd.apache.arrow.file"

    def __init__(
        self,
        *,
        write_options: pa.ipc.IpcReadOptions | None = None,
        read_options: pa.ipc.IpcReadOptions | None = None,
    ) -> None:
        self._write_options = write_options
        self._read_options = read_options


class ArrowTableSerializer(_ArrowTableBase, ValueSerializer[pa.Table]):
    """Serialize a PyArrow table to the arrow file format."""

    name = "lakery.pyarrow.arrow.file"
    version = 1
    types = (pa.Table,)

    def dump_value(self, value: pa.Table) -> ValueDump:
        """Serialize the given Arrow table."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, value.schema, options=self._write_options) as writer:
            writer.write_table(value)
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_value": sink.getvalue().to_pybytes(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> pa.Table:
        """Deserialize the given Arrow table."""
        return pa.ipc.open_file(
            pa.BufferReader(dump["content_value"]), options=self._read_options
        ).read_all()


class ArrowRecordBatchStreamSerializer(_ArrowTableBase, StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the arrow stream format."""

    name = "lakery.arrow.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)

    def dump_value(self, value: Iterable[pa.RecordBatch]) -> ValueDump:
        """Serialize the given stream of Arrow record batches."""
        buffer = io.BytesIO()
        value_iter = iter(value)
        item = next(value_iter)
        with pa.ipc.new_stream(buffer, item.schema, options=self._write_options) as writer:
            writer.write_batch(item)
            for item in value_iter:
                writer.write_batch(item)
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_value": buffer.getvalue(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> Iterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return pa.ipc.open_stream(dump["content_value"], options=self._read_options)

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "content_encoding": None,
            "content_stream": _dump_arrow_record_batch_stream(stream, self._write_options),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncGenerator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_arrow_record_batch_stream(dump["content_stream"])


class ParquetWriteOptions(TypedDict, total=False):
    """See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html."""

    version: str
    use_dictionary: bool | list
    compression: Literal["NONE", "SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD"] | dict
    write_statistics: bool | list
    coerce_timestamps: str
    allow_truncated_timestamps: bool
    data_page_size: int
    flavor: str
    filesystem: fs.FileSystem
    compression_level: int | dict
    use_byte_stream_split: bool | list
    column_encoding: str | dict
    data_page_version: str
    use_compliant_nested_type: bool
    encryption_properties: Any
    write_batch_size: int
    dictionary_pagesize_limit: int
    store_schema: bool
    write_page_index: bool
    write_page_checksum: bool
    sorting_columns: Sequence[pq.SortingColumn]
    store_decimal_as_integer: bool


class ParquetReadOptions(TypedDict, total=False):
    """See https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html."""

    metadata: pq.FileMetaData
    common_metadata: pq.FileMetaData
    read_dictionary: list
    memory_map: bool
    buffer_size: int
    pre_buffer: bool
    coerce_int96_timestamp_unit: str
    decryption_properties: Any
    thrift_string_size_limit: int
    thrift_container_size_limit: int
    filesystem: fs.FileSystem
    page_checksum_verification: bool


class ParquetTableSerializer(ValueSerializer[pa.Table]):
    """Serialize a PyArrow table to the parquet file format."""

    name = "lakery.pyarrow.parquet"
    version = 1
    types = (pa.Table,)
    content_type = "application/vnd.apache.parquet"

    def __init__(
        self,
        *,
        write_options: ParquetWriteOptions | None = None,
        write_option_extras: Mapping[str, Any] | None = None,
        read_options: ParquetReadOptions | None = None,
    ) -> None:
        self.write_options = write_options or {}
        self.write_option_extras = write_option_extras or {}
        self.read_options = read_options or {}

    def dump_value(self, value: pa.Table) -> ValueDump:
        """Serialize the given Arrow table."""
        buffer = io.BytesIO()
        with pq.ParquetWriter(
            buffer,
            value.schema,
            **self.write_options,  # type: ignore[reportArgumentType]
            **self.write_option_extras,
        ) as writer:
            writer.write_table(value)
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_value": buffer.getvalue(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> pa.Table:
        """Deserialize the given Arrow table."""
        return pq.ParquetFile(pa.BufferReader(dump["content_value"]), **self.read_options).read()


class ParquetRecordBatchStreamSerializer(StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the parquet file format."""

    name = "lakery.arrow.parquet.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)
    content_type = "application/vnd.apache.parquet"

    def __init__(
        self,
        *,
        write_options: ParquetWriteOptions | None = None,
        write_option_extras: Mapping[str, Any] | None = None,
        read_options: ParquetReadOptions | None = None,
    ) -> None:
        self.write_options = write_options or {}
        self.write_option_extras = write_option_extras or {}
        self.read_options = read_options or {}

    def dump_value(self, value: Iterable[pa.RecordBatch]) -> ValueDump:
        """Serialize the given stream of Arrow record batches."""
        buffer = io.BytesIO()
        value_iter = iter(value)
        item = next(value_iter)
        with pq.ParquetWriter(
            buffer,
            item.schema,
            **self.write_options,  # type: ignore[reportArgumentType]
            **self.write_option_extras,
        ) as writer:
            writer.write_batch(item)
            for item in value_iter:
                writer.write_batch(item)
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "content_value": buffer.getvalue(),
        }

    def load_value(self, dump: ValueDump) -> Iterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        with pq.ParquetFile(pa.BufferReader(dump["content_value"]), **self.read_options) as reader:
            for row_group_index in range(reader.num_row_groups):
                row_group: pa.Table = reader.read_row_group(row_group_index)
                yield from row_group.to_batches()

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
            "content_stream": _dump_parquet_record_batch_stream(
                stream,
                self.write_options,
                self.write_option_extras,
            ),
        }

    def load_stream(self, dump: StreamDump) -> AsyncGenerator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_parquet_record_batch_stream(dump["content_stream"], self.read_options)


async def _dump_parquet_record_batch_stream(
    record_batch_stream: AsyncIterable[pa.RecordBatch],
    write_options: ParquetWriteOptions,
    write_option_extras: Mapping[str, Any],
) -> AsyncGenerator[bytes]:
    buffer = io.BytesIO()
    stream_writer = None
    with ExitStack() as stack:
        async for batch in record_batch_stream:
            if stream_writer is None:
                stream_writer = stack.enter_context(
                    pq.ParquetWriter(
                        buffer,
                        batch.schema,
                        **write_options,  # type: ignore[reportArgumentType]
                        **write_option_extras,
                    )
                )
            stream_writer.write_batch(batch)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()
    yield buffer.getvalue()


async def _load_parquet_record_batch_stream(
    byte_stream: AsyncIterable[bytes],
    read_options: ParquetReadOptions,
) -> AsyncGenerator[pa.RecordBatch]:
    buffer = io.BytesIO()
    stream_reader = None
    async for byte in byte_stream:
        buffer.write(byte)
        end = buffer.tell()
        buffer.seek(0)
        if stream_reader is None:
            try:
                stream_reader = pq.ParquetFile(buffer, **read_options)
            except pa.ArrowInvalid:
                buffer.seek(end)
                continue
        for row_group_index in range(stream_reader.num_row_groups):
            row_group: pa.Table = stream_reader.read_row_group(row_group_index)
            for batch in row_group.to_batches():
                yield batch
        remainder = buffer.read()
        buffer.seek(0)
        buffer.write(remainder)
        buffer.truncate()


async def _dump_arrow_record_batch_stream(
    batch_stream: AsyncIterable[pa.RecordBatch],
    options: pa.ipc.IpcWriteOptions | None,
) -> AsyncGenerator[bytes]:
    writer = None
    buffer = io.BytesIO()
    with ExitStack() as stack:
        async for batch in batch_stream:
            if writer is None:
                writer = stack.enter_context(
                    pa.ipc.new_stream(buffer, batch.schema, options=options)
                )
            writer.write_batch(batch)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()


async def _load_arrow_record_batch_stream(
    byte_stream: AsyncIterable[bytes],
) -> AsyncGenerator[pa.RecordBatch]:
    schema: pa.Schema | None = None
    byte_stream_iter = aiter(byte_stream)
    async for msg in _AsyncMessageReader(byte_stream_iter):
        if schema is None:
            # first message must be a schema
            schema = pa.ipc.read_schema(msg.serialize())
        else:
            yield pa.ipc.read_record_batch(msg.serialize(), schema)
    # ensure the stream is consumed
    async for _ in byte_stream_iter:
        pass


class _AsyncMessageReader(AsyncIterator[pa.Message]):
    """Wraps an async iterable of bytes into an async iterable of PyArrow IPC messages.

    Copied from: https://gist.github.com/gatesn/86462c33d765b0fc63d7bb88529204d0
    """

    def __init__(self, bytes_iter: AsyncIterator[bytes]):
        self._bytes_iter = bytes_iter

        self._buffer = bytearray()

    async def read_next_message(self) -> pa.Message:
        """Read the next message from the stream."""
        return await anext(self)

    async def __anext__(self):
        # First parse the IPC encapsulation header
        # See: https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
        await self._ensure_bytes(8)
        if self._buffer[:4] != b"\xff\xff\xff\xff":
            msg = "Invalid IPC stream - expected continuation bytes"
            raise ValueError(msg)
        header_len = int.from_bytes(self._buffer[4:8], "little")

        # Check for end-of-stream marker
        if not header_len:
            raise StopAsyncIteration

        # Fetch the Arrow FlatBuffers Message header
        await self._ensure_bytes(8 + header_len)

        # Parse the bodyLength out of the flatbuffers Message.
        body_len = self._parse_body_len(memoryview(self._buffer)[8:header_len])
        total_len = 8 + header_len + body_len
        await self._ensure_bytes(total_len)

        # Parse the IPC message and reset the buffer
        msg = pa.ipc.read_message(memoryview(self._buffer)[:total_len])
        self._buffer = bytearray(self._buffer[total_len:])
        return msg

    async def _ensure_bytes(self, n: int):
        while len(self._buffer) < n:
            self._buffer.extend(await anext(self._bytes_iter))

    @staticmethod
    def _parse_body_len(header: memoryview) -> int:
        """Extract the body length from a raw arrow flatbuffer Message.

        See: https://github.com/apache/arrow/blob/main/format/Message.fbs
        See: https://github.com/dvidelabs/flatcc/blob/master/doc/binary-format.md#internals
        """
        root_table = int.from_bytes(header[:4], "little", signed=True)

        vtable = root_table - int.from_bytes(header[root_table:][:4], "little", signed=True)
        vtable_len = int.from_bytes(header[vtable:][:2], "little")

        # We know bodyLength lives at offset 10 within the vtable
        # (verified against generated FlatBuffers code).
        #
        #   table Message {
        #     version: org.apache.arrow.flatbuf.MetadataVersion;
        #     header: MessageHeader;
        #     bodyLength: long;
        #     custom_metadata: [ KeyValue ];
        #   }
        #
        #   0 => table length
        #   4 => version
        #   6 => HeaderType
        #   8 => Header
        #   10 => BodyLength
        #   12 => CustomMetadata
        #
        body_len_vtable_offset = 10

        # If the vtable is too short, then the body hasn't been set.
        if vtable_len <= body_len_vtable_offset:
            return 0

        # Otherwise, use the vtable to get the location of the body_length in order to then read it.
        body_len_offset = int.from_bytes(header[vtable + body_len_vtable_offset :][:2], "little")
        return int.from_bytes(header[root_table + body_len_offset :][:8], "little")
