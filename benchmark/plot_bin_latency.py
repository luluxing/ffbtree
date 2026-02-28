#!/usr/bin/env python3

import argparse
import struct
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt

# Binary layout written by benchmark_cc.cpp
RECORD_STRUCT = struct.Struct("<qii")  # latency_ns (int64), thread_id (int32), restart_count (int32)
EXPECTED_FIELDS = ["latency_ns", "thread_id", "restart_count (-1 for lookups)"]


def find_unique_file(directory: Path, prefix: str, index: int, extension: str) -> Path:
    pattern = f"{prefix}*index-{index}*{extension}"
    matches = sorted(p for p in directory.glob(pattern) if p.is_file())
    if not matches:
        raise FileNotFoundError(f"No {extension} file found for prefix '{prefix}' and index-{index} in {directory}")
    if len(matches) > 1:
        raise ValueError(f"Multiple {extension} files found for prefix '{prefix}' and index-{index} in {directory}: {matches}")
    return matches[0]


def read_header_fields(out_path: Path) -> List[str]:
    for line in out_path.read_text().splitlines():
        if line.startswith("Header:"):
            return [field.strip() for field in line.split("Header:", 1)[1].split(",") if field.strip()]
    return []


def load_latencies(bin_path: Path) -> List[int]:
    raw = bin_path.read_bytes()
    if len(raw) % RECORD_STRUCT.size != 0:
        raise ValueError(f"File size of {bin_path} is not a multiple of record size {RECORD_STRUCT.size}")
    return [latency for latency, _, _ in RECORD_STRUCT.iter_unpack(raw)]


def downsample(values: Iterable[int], step: int, limit: Optional[int]) -> Tuple[List[int], List[int]]:
    if step <= 0:
        raise ValueError("sample_step must be a positive integer")
    xs: List[int] = []
    ys: List[int] = []
    for idx, val in enumerate(values):
        if limit is not None and idx >= limit:
            break
        if idx % step == 0:
            xs.append(idx + 1)  # x-axis is the number of data entries
            ys.append(val)
    return xs, ys


def plot(
    lat0: List[int],
    lat1: List[int],
    title: str,
    output: Path,
    sample_step: int,
    max_points: Optional[int],
    show: bool,
) -> None:
    x0, y0 = downsample(lat0, sample_step, max_points)
    x1, y1 = downsample(lat1, sample_step, max_points)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x0, y0, label="prebtree (index-0)", alpha=0.8)
    ax.plot(x1, y1, label="ffbtree (index-1)", alpha=0.8)
    ax.set_xlabel("Number of data entries")
    ax.set_ylabel("Latency (ns)")
    ax.set_title(title)
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.savefig(output, dpi=200)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot latencies from paired .bin files (index-0 prebtree, index-1 ffbtree) using the header found in the .out file."
    )
    parser.add_argument("prefix", help="File prefix used before 'index-X' in the benchmark output names")
    parser.add_argument(
        "--directory",
        default=Path(__file__).parent,
        type=Path,
        help="Directory containing the benchmark .bin/.out files (default: benchmark directory)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Path for the output plot (default: <directory>/<prefix>_latency.png)",
    )
    parser.add_argument(
        "--sample-step",
        type=int,
        default=1,
        help="Use every Nth data point to reduce plot density (default: 1, i.e., plot all points)",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=None,
        help="Optional cap on the number of records to read from each file before plotting",
    )
    parser.add_argument("--show", action="store_true", help="Display the plot window in addition to saving the file")
    args = parser.parse_args()

    base_dir = args.directory
    prefix = args.prefix

    bin_pre = find_unique_file(base_dir, prefix, index=0, extension=".bin")
    bin_ff = find_unique_file(base_dir, prefix, index=1, extension=".bin")

    # Confirm header layout from any matching .out file
    out_pre = find_unique_file(base_dir, prefix, index=0, extension=".out")
    header_fields = read_header_fields(out_pre)
    if header_fields and header_fields != EXPECTED_FIELDS:
        raise ValueError(f"Unexpected header fields {header_fields} in {out_pre}, expected {EXPECTED_FIELDS}")

    lat_pre = load_latencies(bin_pre)
    lat_ff = load_latencies(bin_ff)

    output_path = args.output
    if output_path is None:
        safe_prefix = prefix.replace("/", "_")
        output_path = base_dir / f"{safe_prefix}_latency.png"

    plot(lat_pre, lat_ff, title=prefix, output=output_path, sample_step=args.sample_step, max_points=args.max_points, show=args.show)
    print(f"Saved plot to {output_path}")


if __name__ == "__main__":
    main()
