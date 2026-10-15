"""Shared plumbing: one DataSource factory, parameterised by a `fetch(url, offset, limit)` function.

`fetch` returns either a list of tuples (row path) or a pyarrow.RecordBatch (Arrow path).
Options: url, total_rows, partitions, page_size.
"""
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

SCHEMA = "id long, value double, name string, ts long"


class PageRange(InputPartition):
    def __init__(self, start, stop):
        self.start, self.stop = start, stop


class HttpReader(DataSourceReader):
    def __init__(self, options, fetch):
        self.url = options["url"].rstrip("/")
        self.total = int(options.get("total_rows", 100_000))
        self.nparts = int(options.get("partitions", 4))
        self.page = int(options.get("page_size", 10_000))
        self.fetch = fetch

    def partitions(self):
        step = -(-self.total // self.nparts)
        return [PageRange(s, min(s + step, self.total)) for s in range(0, self.total, step)]

    def read(self, partition):
        for off in range(partition.start, partition.stop, self.page):
            res = self.fetch(self.url, off, min(self.page, partition.stop - off))
            if isinstance(res, list):
                yield from res
            else:
                yield res  # pyarrow.RecordBatch


def make_source(name, fetch):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return SCHEMA

        def reader(self, schema):
            return HttpReader(self.options, fetch)

    _Source.__name__ = f"HttpSource_{name}"
    return _Source


# ------------------------------------------------------------------ compressed access logs
LOG_SCHEMA = "ip string, ts long, method string, path string, status int, bytes long"


class LogFile(InputPartition):
    def __init__(self, path):
        self.path = path


class LogReader(DataSourceReader):
    """One partition per `*.log.gz` file under option `path`."""

    def __init__(self, options, parse):
        self.dir = options["path"]
        self.parse = parse

    def partitions(self):
        import glob
        import os

        return [LogFile(p) for p in sorted(glob.glob(os.path.join(self.dir, "*.log.gz")))]

    def read(self, partition):
        res = self.parse(partition.path)
        if isinstance(res, list):
            yield from res
        else:
            yield res


def make_log_source(name, parse):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return LOG_SCHEMA

        def reader(self, schema):
            return LogReader(self.options, parse)

    _Source.__name__ = f"LogSource_{name}"
    return _Source


# ------------------------------------------------------------------ NDJSON writers
from pyspark.sql.datasource import DataSourceArrowWriter, DataSourceWriter, WriterCommitMessage  # noqa: E402


class Written(WriterCommitMessage):
    def __init__(self, rows):
        self.rows = rows


def _part_path(directory):
    import os
    import uuid

    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"part-{uuid.uuid4().hex}.ndjson")


def _success(directory, messages):
    import os

    with open(os.path.join(directory, "_SUCCESS"), "w") as f:
        f.write(str(sum(m.rows for m in messages if m)))


class RowNdjsonWriter(DataSourceWriter):
    """Receives pyspark Rows; `encode(rows_iter, path) -> count`."""

    def __init__(self, directory, encode):
        self.directory, self.encode = directory, encode

    def write(self, iterator):
        return Written(self.encode(iterator, _part_path(self.directory)))

    def commit(self, messages):
        _success(self.directory, messages)


class ArrowNdjsonWriter(DataSourceArrowWriter):
    """Receives pyarrow RecordBatches; `encode(batch, path) -> count` appends to the file."""

    def __init__(self, directory, encode):
        self.directory, self.encode = directory, encode

    def write(self, iterator):
        path, n = _part_path(self.directory), 0
        for batch in iterator:
            n += self.encode(batch, path)
        return Written(n)

    def commit(self, messages):
        _success(self.directory, messages)


def make_writer_source(name, encode, arrow):
    cls = ArrowNdjsonWriter if arrow else RowNdjsonWriter

    class _Sink(DataSource):
        @classmethod
        def name(cls_):
            return name

        def writer(self, schema, overwrite):
            return cls(self.options["path"], encode)

    _Sink.__name__ = f"NdjsonSink_{name}"
    return _Sink
