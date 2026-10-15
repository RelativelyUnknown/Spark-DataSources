"""Deterministic gzip'd access-log generator: `[epoch]` timestamps, nginx-ish lines."""
import gzip
import os

METHODS = ("GET", "POST", "PUT", "DELETE")
STATUS = (200, 200, 200, 201, 301, 404, 500)


def line(i):
    return (f'10.{i % 7}.{i % 251}.{i % 13} - - [{1_700_000_000 + i}] '
            f'"{METHODS[i % 4]} /api/v1/item/{i % 977}?q={i % 31} HTTP/1.1" {STATUS[i % 7]} {(i * 37) % 100_000}')


def generate(directory, files=4, lines_per_file=250_000):
    os.makedirs(directory, exist_ok=True)
    for f in range(files):
        path = os.path.join(directory, f"access-{f:03d}.log.gz")
        if os.path.exists(path):
            continue
        base = f * lines_per_file
        with gzip.open(path, "wt", compresslevel=3) as fh:
            fh.write("\n".join(line(base + i) for i in range(lines_per_file)) + "\n")
    return directory


if __name__ == "__main__":
    import sys
    print(generate(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])))
