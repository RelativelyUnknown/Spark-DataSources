PY := .venv/bin/python

.PHONY: setup build test bench notebook server clean
setup:            ## create venv, install deps, build the Rust + Cython extensions
	uv venv --python 3.13 .venv || true
	uv sync --python $(PY)
build:            ## rebuild native extensions after editing Rust/Cython
	uv sync --python $(PY) --reinstall-package sparkhttp-rs --reinstall-package sparkhttp-cy
test:
	$(PY) -m pytest -q
bench:
	$(PY) bench/run_bench.py --rows 1000000 --partitions 4 --json bench/results.json
notebook:         ## interactive marimo notebook (edit mode)
	$(PY) -m marimo edit notebooks/benchmark.py
server:           ## standalone mock API on :8765
	$(PY) bench/mock_server.py
clean:
	rm -rf .venv rust_ext/target cython_ext/build bench/results.json
