import io
from collections.abc import AsyncIterable
from contextlib import ExitStack

import pyarrow as pa
import pyarrow.parquet as pq
from anysync.core import AsyncIterator

from ardex.core.serializer import ScalarDump
from ardex.core.serializer import ScalarSerializer
from ardex.core.serializer import StreamDump
from ardex.core.serializer import StreamSerializer


class ArrowTableSerializer(ScalarSerializer[pa.Table]):
    """Serialize a PyArrow table to the arrow file format."""

    name = "ardex.arrow.table.scalar"
    version = 1
    types = (pa.Table,)
    content_type = "application/vnd.apache.arrow.file"

    def dump_scalar(self, value: pa.Table) -> ScalarDump:
        """Serialize the given Arrow table."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, value.schema) as writer:
            writer.write_table(value)
        return {
            "content_scalar": sink.getvalue().to_pybytes(),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_scalar(self, dump: ScalarDump) -> pa.Table:
        """Deserialize the given Arrow table."""
        return pa.ipc.open_file(pa.BufferReader(dump["content_scalar"])).read_all()


class ParquetRecordBatchStreamSerializer(StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the parquet file format."""

    name = "ardex.arrow.parquet.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)
    content_type = "application/vnd.apache.parquet"

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "content_stream": _dump_parquet_record_batch_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncIterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_parquet_record_batch_stream(dump["content_stream"])


class ArrowRecordBatchStreamSerializer(StreamSerializer[pa.RecordBatch]):
    """Serialize a stream of PyArrow record batches to the arrow stream format."""

    name = "ardex.arrow.record_batch.stream"
    version = 1
    types = (pa.RecordBatch,)
    content_type = "application/vnd.apache.arrow.stream"

    def dump_stream(self, stream: AsyncIterable[pa.RecordBatch]) -> StreamDump:
        """Serialize the given stream of Arrow record batches."""
        return {
            "content_stream": _dump_arrow_record_batch_stream(stream),
            "content_type": self.content_type,
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_stream(self, dump: StreamDump) -> AsyncIterator[pa.RecordBatch]:
        """Deserialize the given stream of Arrow record batches."""
        return _load_arrow_record_batch_stream(dump["content_stream"])


async def _dump_parquet_record_batch_stream(
    record_batch_stream: AsyncIterable[pa.RecordBatch],
) -> AsyncIterator[bytes]:
    buffer = io.BytesIO()
    stream_writer = None

    try:
        async for record_batch in record_batch_stream:
            if stream_writer is None:
                # Initialize the writer with the schema of the first batch
                stream_writer = pa.RecordBatchStreamWriter(buffer, record_batch.schema)

            # Write the record batch to the stream
            stream_writer.write_batch(record_batch)

            # Flush the buffer and yield its contents
            buffer.seek(0)
            yield buffer.read()
            buffer.seek(0)
            buffer.truncate(0)

    finally:
        if stream_writer:
            # Ensure the writer is closed properly
            stream_writer.close()

        # If there's any remaining data in the buffer, yield it
        buffer.seek(0)
        remaining_data = buffer.read()
        if remaining_data:
            yield remaining_data


async def _load_parquet_record_batch_stream(
    byte_stream: AsyncIterable[bytes],
) -> AsyncIterator[pa.RecordBatch]:
    buffer = io.BytesIO()
    async for chunk in byte_stream:
        buffer.write(chunk)

        try:
            # Seek to the beginning and attempt to load Parquet data
            buffer.seek(0)
            parquet_file = pq.ParquetFile(buffer)

            for row_group_index in range(parquet_file.num_row_groups):
                row_group: pa.Table = parquet_file.read_row_group(row_group_index)
                for batch in row_group.to_batches():
                    yield batch
            # Reset buffer after successful processing
            buffer.seek(0)
            buffer.truncate(0)
        except pa.ArrowInvalid:
            # If incomplete data, continue buffering
            buffer.seek(buffer.tell())


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
            end = buffer.tell()
            buffer.seek(0)
            yield buffer.read(end)
            buffer.seek(0)


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
