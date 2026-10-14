from .base import make_source

# name -> (module path, attr) resolved lazily so a missing native build only disables that arm
_SPECS = {
    "http_py": ("spark_datasources.pure_python", "fetch_tuples"),
    "http_py_arrow": ("spark_datasources.pure_python", "fetch_batch"),
    "http_rust": ("sparkhttp_rs", "fetch_tuples"),
    "http_rust_arrow": ("sparkhttp_rs", "fetch_batch"),
    "http_cython": ("sparkhttp_cy", "fetch_tuples"),
}


def _load(spec):
    import importlib

    mod, attr = spec
    return getattr(importlib.import_module(mod), attr)


SOURCES = {}
for _name, _spec in _SPECS.items():
    try:
        SOURCES[_name] = make_source(_name, _load(_spec))
    except ImportError:
        pass


def register_all(spark):
    for cls in SOURCES.values():
        spark.dataSource.register(cls)
    return sorted(SOURCES)
