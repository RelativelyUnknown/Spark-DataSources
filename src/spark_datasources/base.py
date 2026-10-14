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
