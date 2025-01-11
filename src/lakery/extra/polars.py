from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import TypedDict

import polars as pl

from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

if TYPE_CHECKING:
    from collections.abc import Sequence

    from polars._typing import ParallelStrategy
    from polars._typing import ParquetCompression


class ParquetDumpArgs(TypedDict, total=False):
    """Arguments for dumping a Polars DataFrame to Parquet."""

    compression: ParquetCompression
    compression_level: int | None
    statistics: bool | str | dict[str, bool]
    row_group_size: int | None
    data_page_size: int | None
    use_pyarrow: bool
    pyarrow_options: dict[str, Any] | None
    partition_by: str | Sequence[str] | None
    partition_chunk_size_bytes: int
    credential_provider: pl.CredentialProviderFunction | Literal["auto"] | None
    retries: int


class ParquetLoadArgs(TypedDict, total=False):
    """Arguments for loading a Polars DataFrame from Parquet."""

    columns: list[int] | list[str] | None
    n_rows: int | None
    row_index_name: str | None
    row_index_offset: int
    parallel: ParallelStrategy
    use_statistics: bool
    hive_partitioning: bool | None
    glob: bool
    try_parse_hive_dates: bool
    rechunk: bool
    low_memory: bool
    credential_provider: pl.CredentialProviderFunction | Literal["auto"] | None
    retries: int
    use_pyarrow: bool
    pyarrow_options: dict[str, Any] | None
    memory_map: bool
    include_file_paths: str | None
    allow_missing_columns: bool


class ParquetDataFrameSerializer(ValueSerializer[pl.DataFrame]):
    """Serializer for Pandas DataFrames using Parquet."""

    name = "lakery.polars.parquet.file"
    version = 1
    types = (pl.DataFrame,)
    content_type = "application/vnd.apache.parquet"

    def __init__(
        self, *, dump_args: ParquetDumpArgs | None = None, load_args: ParquetLoadArgs | None = None
    ) -> None:
        self._dump_args = dump_args or {}
        self._load_args = load_args or {}

    def dump_value(self, value: pl.DataFrame, /) -> ValueDump:
        """Serialize the given DataFrame."""
        buffer = BytesIO()
        value.write_parquet(buffer, **self._dump_args)
        return {
            "content_encoding": None,
            "content_type": self.content_type,
            "content_bytes": buffer.getvalue(),
            "serializer_name": self.name,
            "serializer_version": self.version,
        }

    def load_value(self, dump: ValueDump, /) -> pl.DataFrame:
        """Deserialize the given DataFrame."""
        return pl.read_parquet(BytesIO(dump["content_bytes"]), **self._load_args)
