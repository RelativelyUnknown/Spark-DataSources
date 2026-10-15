import os

import pytest

from spark_datasources.parquet_source import plan_splits

ENGINES = ["parquet_rust", "parquet_pyarrow"]


@pytest.fixture(scope="module")
def pq_dir(spark, tmp_path_factory):
    """Spark-written (default options: snappy, INT96 timestamps) multi-file dataset, several row groups."""
    d = str(tmp_path_factory.mktemp("pq") / "data")
    df = spark.sql("""
        select id, cast(id % 100 as int) i, id / 8 d, concat('user-', id) s,
               cast(id % 2 = 0 as boolean) b, cast(id as decimal(18,2)) dec,
               date_add(date'2020-01-01', cast(id % 1000 as int)) dt,
               timestamp'2024-01-01 00:00:00' + make_interval(0,0,0,0,0,0,id) ts,
               array(id, id + 1) arr, named_struct('a', id, 'b', concat('x', id)) st,
               if(id % 5 = 0, null, id) nullable
        from range(0, 20000)""")
    df.repartition(3).write.option("parquet.block.size", 64 * 1024).parquet(d)
    return d


def sorted_rows(df):
    return sorted(df.collect(), key=lambda r: r["id"])


@pytest.mark.parametrize("fmt", ENGINES)
def test_matches_builtin(spark, pq_dir, fmt):
    ref = spark.read.parquet(pq_dir)
    got = spark.read.format(fmt).option("path", pq_dir).load()
    assert got.schema == ref.schema or [f.simpleString() for f in got.schema] == [f.simpleString() for f in ref.schema]
    assert sorted_rows(got) == sorted_rows(ref)


@pytest.mark.parametrize("fmt", ENGINES)
def test_split_planning_like_spark(spark, pq_dir, fmt):
    # small maxPartitionBytes -> many partitions; default -> bounded by parallelism
    small = spark.read.format(fmt).option("path", pq_dir).option("maxPartitionBytes", 20000).load()
    assert small.rdd.getNumPartitions() > 3
    assert small.count() == 20000
    one = spark.read.format(fmt).option("path", pq_dir).option("parallelism", 1).load()
    assert one.rdd.getNumPartitions() == 1 and one.count() == 20000


def test_plan_splits_unit():
    files = {"a": [(0, 100), (1, 100)], "b": [(0, 100), (1, 100)]}
    assert len(plan_splits(files, 10**9, 0, 1)) == 1
    assert len(plan_splits(files, 10**9, 0, 4)) == 4
    # one big file is cut into byte ranges holding whole row groups
    assert [len(p[0][1]) for p in plan_splits({"a": [(i, 100) for i in range(8)]}, 10**9, 0, 4)] == [2, 2, 2, 2]
    flat = [(p, r) for part in plan_splits(files, 10**9, 0, 2) for p, rs in part for r in rs]
    assert sorted(flat) == [("a", 0), ("a", 1), ("b", 0), ("b", 1)]


@pytest.mark.parametrize("fmt", ENGINES)
def test_options(spark, pq_dir, tmp_path, fmt):
    d = tmp_path / "tree"
    (d / "sub").mkdir(parents=True)
    src = sorted(x for x in os.listdir(pq_dir) if x.endswith(".parquet"))[0]
    for target in (d / src, d / "sub" / src, d / "_hidden.parquet", d / ".dot.parquet"):
        target.write_bytes(open(os.path.join(pq_dir, src), "rb").read())
    n = spark.read.parquet(str(d / src)).count()

    def rd(**o):  # fresh reader each time: DataFrameReader.option mutates
        r = spark.read.format(fmt).option("path", str(d))
        for k, v in o.items():
            r = r.option(k, v)
        return r.load()

    assert rd().count() == n                                       # hidden + sub-dirs skipped
    assert rd(recursiveFileLookup="true").count() == 2 * n
    (d / "bad.parquet").write_bytes(b"not parquet")
    with pytest.raises(Exception):
        rd().count()
    assert rd(ignoreCorruptFiles="true").count() == n


@pytest.mark.parametrize("fmt", ENGINES)
def test_merge_schema_and_missing_columns(spark, tmp_path, fmt):
    d = str(tmp_path / "m")
    spark.range(0, 10).selectExpr("id", "id * 2 as a").write.parquet(d + "/p1")
    spark.range(10, 20).selectExpr("id", "cast(id as string) as B").write.parquet(d + "/p2")
    rd = lambda **o: sorted_rows(
        spark.read.format(fmt).option("path", d).option("recursiveFileLookup", "true")
        .options(**o).load())
    ref = sorted_rows(spark.read.option("mergeSchema", "true").option("recursiveFileLookup", "true").parquet(d))
    assert rd(mergeSchema="true") == ref
    cols = {c for c in spark.read.format(fmt).option("path", d).option("recursiveFileLookup", "true").load().columns}
    assert len(cols) == 2   # without mergeSchema: footer of one file only


@pytest.mark.parametrize("fmt", ENGINES)
def test_column_pruning_via_user_schema(spark, pq_dir, fmt):
    ref = spark.read.parquet(pq_dir).select("d", "ID", "ts")
    got = (spark.read.format(fmt).schema("d double, ID long, ts timestamp").option("path", pq_dir).load())
    assert got.columns == ["d", "ID", "ts"]
    assert sorted(got.collect(), key=lambda r: r.ID) == sorted(ref.collect(), key=lambda r: r.ID)
