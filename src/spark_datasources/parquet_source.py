"""Parquet file data source with Spark's `spark.read.parquet` defaults, two engines:

* ``parquet_rust``    : arrow-rs `parquet` crate via sparkhttp_rs (GIL released while decoding)
* ``parquet_pyarrow`` : pyarrow.parquet (C++), the "what you'd write in Python" reference

Options (name -> Spark default mirrored):
  path                       file, directory, glob, or comma-separated list      (required)
  mergeSchema                false  - infer from the first file's footer; true = union of all footers
  pathGlobFilter             none   - glob applied to file *names*
  recursiveFileLookup        false  - descend into sub-directories
  ignoreCorruptFiles         false  - skip files whose footer cannot be read
  ignoreMissingFiles         false  - skip files deleted between listing and reading
  maxPartitionBytes          134217728 (spark.sql.files.maxPartitionBytes)
  openCostInBytes            4194304   (spark.sql.files.openCostInBytes)
  minPartitionNum / parallelism      default = CPU count (stand-in for defaultParallelism)

Behaviour matching Spark: files named `_*` / `.*` are ignored; INT96 and any timestamp unit read as
timestamp[us, UTC]; every column nullable; columns matched by name case-insensitively and missing columns
read as NULL; split planning follows FilePartition (largest first, open cost per piece, same split-size
formula, row-group granularity).
NOT supported yet: hive-style partition discovery (k=v dirs), datetime rebase modes, column pruning and
filter pushdown (Python data sources receive neither), modifiedBefore/After.
"""
import fnmatch
import glob as _glob
import os

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

DEFAULT_MAX_PARTITION_BYTES = 128 * 1024 * 1024
DEFAULT_OPEN_COST = 4 * 1024 * 1024
BATCH_ROWS = 65536


def _bool(options, key, default=False):
    v = options.get(key)
    return default if v is None else str(v).lower() == "true"


# ------------------------------------------------------------------ engines
def _rust_inspect(path):
    import sparkhttp_rs

    return sparkhttp_rs.parquet_inspect(path)


def _rust_batches(path, row_groups, columns=None):
    import sparkhttp_rs

    return sparkhttp_rs.parquet_batches(path, row_groups, BATCH_ROWS, columns)


def _spark_type(t):
    P = pa.types
    if P.is_timestamp(t):
        return pa.timestamp("us", tz="UTC")
    if P.is_list(t):
        return pa.list_(pa.field(t.value_field.name, _spark_type(t.value_type), True))
    if P.is_large_list(t):
        return pa.large_list(pa.field(t.value_field.name, _spark_type(t.value_type), True))
    if P.is_struct(t):
        return pa.struct([pa.field(f.name, _spark_type(f.type), True) for f in t])
    if P.is_map(t):
        return pa.map_(_spark_type(t.key_type), pa.field("value", _spark_type(t.item_type), True))
    return t


def spark_schema(schema):
    """pyarrow schema -> Spark-compatible (timestamps us/UTC, all fields nullable, recursively)."""
    return pa.schema([pa.field(f.name, _spark_type(f.type), True) for f in schema])


def _pyarrow_inspect(path):
    import pyarrow.parquet as pq

    f = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    md = f.metadata
    rgs = [(i, sum(md.row_group(i).column(c).total_compressed_size for c in range(md.num_columns)),
            md.row_group(i).num_rows) for i in range(md.num_row_groups)]
    return spark_schema(f.schema_arrow), rgs


def _pyarrow_batches(path, row_groups, columns=None):
    import pyarrow.parquet as pq

    f = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    names = f.schema_arrow.names
    if columns is not None:  # case-insensitive, like spark.sql.caseSensitive=false
        want = {c.lower() for c in columns}
        columns = [n for n in names if n.lower() in want]
    target = spark_schema(f.schema_arrow)
    if columns is not None:
        target = pa.schema([target.field(n) for n in columns])
    for b in f.iter_batches(batch_size=BATCH_ROWS, row_groups=row_groups, columns=columns):
        yield pa.RecordBatch.from_arrays(
            [c.cast(t.type) if c.type != t.type else c for c, t in zip(b.columns, target)], schema=target)


ENGINES = {"rust": (_rust_inspect, _rust_batches), "pyarrow": (_pyarrow_inspect, _pyarrow_batches)}


# ------------------------------------------------------------------ type mapping (pyarrow -> Spark DDL)
def _ddl(t):
    P = pa.types
    if P.is_boolean(t): return "boolean"
    if P.is_int8(t) or P.is_uint8(t): return "tinyint" if P.is_int8(t) else "smallint"
    if P.is_int16(t): return "smallint"
    if P.is_int32(t): return "int"
    if P.is_int64(t): return "bigint"
    if P.is_uint16(t): return "int"
    if P.is_uint32(t): return "bigint"
    if P.is_float32(t): return "float"
    if P.is_float64(t): return "double"
    if P.is_string(t) or P.is_large_string(t): return "string"
    if P.is_binary(t) or P.is_large_binary(t): return "binary"
    if P.is_date32(t): return "date"
    if P.is_timestamp(t): return "timestamp"
    if P.is_decimal(t): return f"decimal({t.precision},{t.scale})"
    if P.is_list(t) or P.is_large_list(t): return f"array<{_ddl(t.value_type)}>"
    if P.is_struct(t):
        return "struct<" + ",".join(f"`{f.name}`:{_ddl(f.type)}" for f in t) + ">"
    if P.is_map(t): return f"map<{_ddl(t.key_type)},{_ddl(t.item_type)}>"
    raise ValueError(f"unsupported parquet type for Spark: {t}")


def to_ddl(schema):
    return ", ".join(f"`{f.name}` {_ddl(f.type)}" for f in schema)


# ------------------------------------------------------------------ file discovery
def list_files(options):
    """Spark's InMemoryFileIndex rules: hidden (_ / .) names are skipped, optional glob + recursion."""
    recursive = _bool(options, "recursiveFileLookup")
    name_glob = options.get("pathGlobFilter")
    out = []
    for root in str(options["path"]).split(","):
        root = root.strip()
        for hit in sorted(_glob.glob(root)) or [root]:
            if os.path.isdir(hit):
                if recursive:
                    cand = [os.path.join(d, f) for d, ds, fs in os.walk(hit)
                            for f in fs if not any(p.startswith(("_", ".")) for p in
                                                    os.path.relpath(os.path.join(d, f), hit).split(os.sep))]
                else:
                    cand = [os.path.join(hit, f) for f in os.listdir(hit)
                            if os.path.isfile(os.path.join(hit, f)) and not f.startswith(("_", "."))]
            else:
                cand = [hit]
            out += [c for c in cand if not name_glob or fnmatch.fnmatch(os.path.basename(c), name_glob)]
    return sorted(set(out))


# ------------------------------------------------------------------ planning
class ParquetSplit(InputPartition):
    """A bundle of (path, [row groups]) pieces read by one task."""

    def __init__(self, pieces):
        self.pieces = pieces


def plan_splits(files, max_bytes, open_cost, parallelism):
    """files: {path: [(row_group, bytes), ...]} -> [[(path, [row_groups])]] mirroring Spark's
    FileSourceScanExec: maxSplitBytes = min(maxPartitionBytes, max(openCost, total/parallelism)); each file
    is cut into byte ranges of that size (a row group belongs to the range holding its start offset), then
    ranges are packed largest-first, charging `open_cost` per range."""
    total = sum(sum(b for _, b in rgs) + open_cost for rgs in files.values())
    max_split = max(1, min(max_bytes, max(open_cost, total // max(parallelism, 1))))
    ranges = []  # (size, path, [row groups])
    for path, rgs in files.items():
        length, offsets, off = sum(b for _, b in rgs), [], 0
        for rg, b in rgs:
            offsets.append((off, rg))
            off += b
        for start in range(0, max(length, 1), max_split):
            members = [rg for o, rg in offsets if start <= o < start + max_split]
            if members:
                ranges.append((min(max_split, length - start), path, members))
    bins, cur, cur_size = [], [], 0
    for size, path, members in sorted(ranges, key=lambda r: -r[0]):
        if cur and cur_size + size > max_split:
            bins.append(cur)
            cur, cur_size = [], 0
        cur.append((path, members))
        cur_size += size + open_cost
    if cur:
        bins.append(cur)
    return bins


def _lower_index(schema):
    return {f.name.lower(): i for i, f in enumerate(schema)}


def merge_schemas(schemas):
    """Union by (case-insensitive) name in first-seen order; widening is not attempted (like Spark: error)."""
    fields, seen = [], {}
    for s in schemas:
        for f in s:
            k = f.name.lower()
            if k not in seen:
                seen[k] = f
                fields.append(f)
            elif seen[k].type != f.type:
                raise ValueError(f"mergeSchema: conflicting types for '{f.name}': {seen[k].type} vs {f.type}")
    return pa.schema(fields)


def align(batch, target):
    """Match a file batch to the table schema by name (case-insensitive); absent columns become NULL."""
    if batch.schema.equals(target, check_metadata=False):
        return batch
    idx = _lower_index(batch.schema)
    cols = []
    for f in target:
        i = idx.get(f.name.lower())
        cols.append(batch.column(i) if i is not None else pa.nulls(batch.num_rows, f.type))
    return pa.RecordBatch.from_arrays(cols, schema=target)


# ------------------------------------------------------------------ data source
class _Scan:
    """Driver-side: list files, read footers, derive schema + splits (cached per DataSource)."""

    def __init__(self, options, engine):
        self.options, self.engine = options, engine
        inspect, _ = ENGINES[engine]
        ignore_corrupt = _bool(options, "ignoreCorruptFiles")
        ignore_missing = _bool(options, "ignoreMissingFiles")
        merge = _bool(options, "mergeSchema")
        files = list_files(options)
        if not files:
            raise FileNotFoundError(f"no parquet files found at {options['path']}")
        schemas, self.files = [], {}
        for path in files:  # every footer is needed for row-group sizes; schema = first (or union)
            try:
                schema, rgs = inspect(path)
            except Exception as e:
                missing = isinstance(e, FileNotFoundError) or "No such file" in str(e)
                if (missing and ignore_missing) or (not missing and ignore_corrupt):
                    continue
                raise
            schemas.append(schema)
            self.files[path] = [(rg, max(b, 0)) for rg, b, n in rgs if n > 0]
        if not schemas:
            raise FileNotFoundError(f"no readable parquet files at {options['path']}")
        self.schema = merge_schemas(schemas) if merge else schemas[0]


def make_parquet_source(name, engine):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def _scan(self):
            if not hasattr(self, "_cached"):
                self._cached = _Scan(self.options, engine)
            return self._cached

        def schema(self):
            return to_ddl(self._scan().schema)

        def reader(self, schema):
            return ParquetReader(self.options, engine, self._scan(), schema)

    _Source.__name__ = f"ParquetSource_{name}"
    return _Source


class ParquetReader(DataSourceReader):
    def __init__(self, options, engine, scan, schema=None):
        self.engine = engine
        # Column pruning: Spark hands us the schema the user asked for (`.schema(...)`); when it is a
        # subset of the table schema only those columns are decoded. (Python data sources never get
        # automatic pruning from the query, unlike the built-in reader.)
        by_name = {f.name.lower(): f for f in scan.schema}
        names = [f.name for f in schema.fields] if schema is not None else [f.name for f in scan.schema]
        missing = [n for n in names if n.lower() not in by_name]
        if missing:
            raise ValueError(f"columns not in parquet schema: {missing}")
        self.target = pa.schema([by_name[n.lower()].with_name(n) for n in names])
        self.columns = None if len(names) == len(scan.schema) else names
        self.ignore_corrupt = _bool(options, "ignoreCorruptFiles")
        self.ignore_missing = _bool(options, "ignoreMissingFiles")
        par = int(options.get("minPartitionNum") or options.get("parallelism") or (os.cpu_count() or 1))
        self._splits = [ParquetSplit(p) for p in plan_splits(
            scan.files, int(options.get("maxPartitionBytes", DEFAULT_MAX_PARTITION_BYTES)),
            int(options.get("openCostInBytes", DEFAULT_OPEN_COST)), par)]

    def partitions(self):
        return self._splits or [ParquetSplit([])]

    def read(self, partition):
        _, batches = ENGINES[self.engine]
        for path, rgs in partition.pieces:
            try:
                for b in batches(path, rgs, self.columns):
                    yield align(b, self.target)
            except FileNotFoundError:
                if not self.ignore_missing:
                    raise
            except Exception:
                if not self.ignore_corrupt:
                    raise
