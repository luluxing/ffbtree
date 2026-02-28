import argparse
import re
import struct
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt

# Binary layout written by benchmark_cc.cpp
RECORD_STRUCT = struct.Struct("<qii")  # latency_ns, thread_id, restart_count
EXPECTED_FIELDS = ["latency_ns", "thread_id", "restart_count"]
palette = { 'NBtree': '#4C78A8', 'PreBtree': '#F58518', 'FFBtree': '#54A24B' }
markers = { 'NBtree': 'o', 'PreBtree': 's', 'FFBtree': 'D' }
line_styles = { 'NBtree': '-', 'PreBtree': '--', 'FFBtree': '-.', 'FFBtree': ':' }

def find_unique_file(directory: Path, prefix: str, index: int, extension: str) -> Path:
    pattern = f"{prefix}*index-{index}*{extension}"
    matches = sorted(p for p in directory.glob(pattern) if p.is_file())
    if not matches:
        raise FileNotFoundError(f"No {extension} file found for prefix '{prefix}' and index-{index} in {directory}")
    if len(matches) > 1:
        raise ValueError(f"Multiple {extension} files found for prefix '{prefix}' and index-{index} in {directory}: {matches}")
    return matches[0]

def find_files(directory: Path, prefix: str, index: int, extension: str) -> Dict[int, Path]:
    """
    Return all files matching pattern `{prefix}_thr-X_index-{index}*{extension}` mapped by thread id.
    """
    pattern = re.compile(
        rf"^{re.escape(prefix)}_thr-(\d+)_index-{index}.*{re.escape(extension)}$"
    )
    matches = sorted(p for p in directory.glob(f"{prefix}_thr-*_index-{index}*{extension}") if p.is_file())
    if not matches:
        raise FileNotFoundError(
            f"No {extension} file found for prefix '{prefix}' and index-{index} in {directory}"
        )

    by_thread: Dict[int, Path] = {}
    for path in matches:
        match = pattern.match(path.name)
        if not match:
            continue
        thread_id = int(match.group(1))
        if thread_id in by_thread:
            raise ValueError(
                f"Multiple files found for prefix '{prefix}', index-{index}, thread-{thread_id}: "
                f"{by_thread[thread_id]}, {path}"
            )
        by_thread[thread_id] = path

    if not by_thread:
        raise ValueError(
            f"Files matched glob for prefix '{prefix}' and index-{index} but none matched expected pattern"
        )
    return by_thread


def load_latencies(bin_path: Path):
    latencies = []
    raw = bin_path.read_bytes()
    if len(raw) % RECORD_STRUCT.size != 0:
        raise ValueError(f"File size of {bin_path} is not a multiple of record size {RECORD_STRUCT.size}")
    count = 0
    for latency, _, _ in RECORD_STRUCT.iter_unpack(raw):
        latencies.append(latency)
        count += 1
        # if count > 10000:
        #     break
    return latencies
    


def plot_latency(
    lat_pre,
    lat_ff,
    title: str,
    output: Path
) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    # Plot the time series
    ax.plot(lat_pre, label='PreBtree', alpha=0.8)
    ax.plot(lat_ff, label='FFBtree', alpha=0.8)
    ax.set_xlabel("Number of data entries")
    ax.set_ylabel("Latency (ns)")
    ax.set_title(title)
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.savefig(output, dpi=200)
    


def plot_restart(
    restart_pre,
    restart_ff,
    title: str,
    output: Path
) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    # Get all thread counts and sort them
    all_threads = sorted(set(restart_pre.keys()) | set(restart_ff.keys()))
    
    # Extract restart counts for each index
    restart_counts_pre = [restart_pre.get(thread_count, 0) for thread_count in all_threads]
    restart_counts_ff = [restart_ff.get(thread_count, 0) for thread_count in all_threads]
    
    # Plot lines connecting points of the same index
    ax.plot(all_threads, restart_counts_pre, 
            label='PreBtree', 
            alpha=0.8, 
            color=palette['PreBtree'], 
            marker=markers['PreBtree'], 
            linestyle=line_styles['PreBtree'],
            linewidth=2,
            markersize=8)
    ax.plot(all_threads, restart_counts_ff, 
            label='FFBtree', 
            alpha=0.8, 
            color=palette['FFBtree'], 
            marker=markers['FFBtree'], 
            linestyle=line_styles['FFBtree'],
            linewidth=2,
            markersize=8)
    
    ax.set_xlabel("Number of threads")
    ax.set_ylabel("Restart count")
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
    args = parser.parse_args()

    base_dir = args.directory
    prefix = args.prefix
    title = prefix

    bin_pre = find_unique_file(base_dir, prefix, index=0, extension=".bin")
    bin_ff = find_unique_file(base_dir, prefix, index=1, extension=".bin")

    lat_pre = load_latencies(bin_pre)
    lat_ff = load_latencies(bin_ff)

    output_path_latency = prefix + "-time_series.png"

    plot_latency(lat_pre, lat_ff, title=title, output=output_path_latency)

if __name__ == "__main__":
    main()
