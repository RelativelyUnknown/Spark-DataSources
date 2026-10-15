import sys, time, statistics
sys.path.insert(0, __import__("os").path.dirname(__file__))
import harness
from pyspark.sql.datasource import DataSource
from spark_datasources.parquet_source import ParquetReader, _Scan, to_ddl, ENGINES, align

_CACHE = {}

class CachedReader(ParquetReader):
    """Same planning/partitions as parquet_rust, but batches are decoded once and replayed from memory."""
    def read(self, partition):
        _, batches = ENGINES["rust"]
        for path, rgs in partition.pieces:
            key = (path, tuple(rgs), tuple(self.columns or ()))
            if key not in _CACHE:
                _CACHE[key] = [align(b, self.target) for b in batches(path, rgs, self.columns)]
            yield from _CACHE[key]

class Floor(DataSource):
    @classmethod
    def name(cls): return "parquet_floor"
    def _scan(self):
        if not hasattr(self, "_c"): self._c = _Scan(self.options, "rust")
        return self._c
    def schema(self): return to_ddl(self._scan().schema)
    def reader(self, schema): return CachedReader(self.options, "rust", self._scan(), schema)

spark = harness.get_spark(); spark.dataSource.register(Floor)
d = harness.make_parquet(spark)
def t(fmt, ddl=None, exprs=("sum(id)","sum(i)","sum(d)","sum(length(s))","count(b)","sum(dec)","max(dt)","max(ts)")):
    def df():
        if fmt == "builtin": return spark.read.parquet(d)
        r = spark.read.format(fmt).option("path", d)
        return (r.schema(ddl) if ddl else r).load()
    run = lambda: df().selectExpr(*exprs).collect()
    run(); run()
    ts = []
    for _ in range(5):
        s = time.perf_counter(); run(); ts.append(time.perf_counter() - s)
    return statistics.median(ts)
for fmt in ("builtin", "parquet_rust", "parquet_floor"):
    print(fmt, "full scan %.3f s" % t(fmt))
