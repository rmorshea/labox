import io
from collections.abc import AsyncIterable
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import ExitStack

import pyarrow as pa
import pyarrow.parquet as pq
from anysync.core import AsyncIterator

from ardex.core.serializer import StreamDump
from ardex.core.serializer import StreamSerializer
from ardex.core.serializer import ValueDump
from ardex.core.serializer import ValueSerializer


class ArrowTableSerializer(ValueSerializer[pa.Table]):
    """Serialize a PyArrow table to the arrow file format."""

    name = "ardex.pyarrow.arrow.file"
    version = 1
    types = (pa.Table,)
    content_type = "application/vnd.apache.arrow.file"

    def dump_value(self, value: pa.Table) -> ValueDump:
        """Serialize the given Arrow table."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, value.schema) as writer:
            writer.write_table(value)
        return {
            "value": sink.getvalue().to_pybytes(),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> pa.Table:
        """Deserialize the given Arrow table."""
        return pa.ipc.open_file(pa.BufferReader(dump["value"])).read_all()


class ParquetTableSerializer(ValueSerializer[pa.Table]):
    """Serialize a PyArrow table to the parquet file format."""

    name = "ardex.pyarrow.parquet"
    version = 1
    types = (pa.Table,)
    content_type = "application/vnd.apache.parquet"

    def dump_value(self, value: pa.Table) -> ValueDump:
        """Serialize the given Arrow table."""
        buffer = io.BytesIO()
        with pq.ParquetWriter(buffer, value.schema) as writer:
            writer.write_table(value)
        return {
            "value": buffer.getvalue(),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> pa.Table:
        """Deserialize the given Arrow table."""
        return pq.ParquetFile(pa.BufferReader(dump["value"])).read()


class ParquetRecordBatchStreamSerializer(StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the parquet file format."""

    name = "ardex.arrow.parquet.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)
    content_type = "application/vnd.apache.parquet"

    def dump_value(self, value: Iterable[pa.RecordBatch]) -> ValueDump:
        """Serialize the given stream of Arrow record batches."""
        buffer = io.BytesIO()
        value_iter = iter(value)
        item = next(value_iter)
        with pq.ParquetWriter(buffer, item.schema) as writer:
            writer.write_batch(item)
            for item in value_iter:
                writer.write_batch(item)
        return {
            "value": buffer.getvalue(),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> Iterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        with pq.ParquetFile(pa.BufferReader(dump["value"])) as reader:
            for row_group_index in range(reader.num_row_groups):
                row_group: pa.Table = reader.read_row_group(row_group_index)
                yield from row_group.to_batches()

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "stream": _dump_parquet_record_batch_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncIterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_parquet_record_batch_stream(dump["stream"])


class ArrowRecordBatchStreamSerializer(StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the arrow stream format."""

    name = "ardex.arrow.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)
    content_type = "application/vnd.apache.arrow.stream"

    def dump_value(self, value: Iterable[pa.RecordBatch]) -> ValueDump:
        """Serialize the given stream of Arrow record batches."""
        buffer = io.BytesIO()
        value_iter = iter(value)
        item = next(value_iter)
        with pa.ipc.new_stream(buffer, item.schema) as writer:
            writer.write_batch(item)
            for item in value_iter:
                writer.write_batch(item)
        return {
            "value": buffer.getvalue(),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump) -> Iterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return pa.ipc.open_stream(dump["value"])

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "stream": _dump_arrow_record_batch_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncIterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_arrow_record_batch_stream(dump["stream"])


async def _dump_parquet_record_batch_stream(
    record_batch_stream: AsyncIterable[pa.RecordBatch],
) -> AsyncIterator[bytes]:
    buffer = io.BytesIO()
    stream_writer = None
    with ExitStack() as stack:
        async for batch in record_batch_stream:
            if stream_writer is None:
                stream_writer = stack.enter_context(pq.ParquetWriter(buffer, batch.schema))
            stream_writer.write_batch(batch)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()
    yield buffer.getvalue()


async def _load_parquet_record_batch_stream(
    byte_stream: AsyncIterable[bytes],
) -> AsyncIterator[pa.RecordBatch]:
    buffer = io.BytesIO()
    stream_reader = None
    async for byte in byte_stream:
        buffer.write(byte)
        end = buffer.tell()
        buffer.seek(0)
        if stream_reader is None:
            try:
                stream_reader = pq.ParquetFile(buffer)
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
) -> AsyncIterator[bytes]:
    writer = None
    buffer = io.BytesIO()
    with ExitStack() as stack:
        async for batch in batch_stream:
            if writer is None:
                writer = stack.enter_context(pa.ipc.new_stream(buffer, batch.schema))
            writer.write_batch(batch)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()


async def _load_arrow_record_batch_stream(
    byte_stream: AsyncIterable[bytes],
) -> AsyncIterator[pa.RecordBatch]:
    schema: pa.Schema | None = None
    async for msg in _AsyncMessageReader(aiter(byte_stream)):
        if schema is None:
            # first message must be a schema
            schema = pa.ipc.read_schema(msg.serialize())
        else:
            yield pa.ipc.read_record_batch(msg.serialize(), schema)


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
