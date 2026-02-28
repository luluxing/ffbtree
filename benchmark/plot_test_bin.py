#!/usr/bin/env python3
"""
Plot latency vs row number from build/test.bin binary file.
The binary file contains records of (latency_ns, io_num, height).
"""

import struct
from pathlib import Path
import matplotlib.pyplot as plt

# Binary layout: latency_ns (long long), io_num (int), height (int)
RECORD_STRUCT = struct.Struct("<qii")  # < = little-endian, q = long long, i = int


def load_latencies(bin_path: Path):
    """Load latency values from binary file."""
    latencies = []
    raw = bin_path.read_bytes()
    
    if len(raw) % RECORD_STRUCT.size != 0:
        raise ValueError(
            f"File size of {bin_path} ({len(raw)} bytes) is not a multiple of "
            f"record size {RECORD_STRUCT.size} bytes"
        )
    
    for latency, _, _ in RECORD_STRUCT.iter_unpack(raw):
        latencies.append(latency)
    
    return latencies


def plot_latency_vs_row(latencies, output_path: Path):
    """Plot latency vs row number."""
    row_numbers = list(range(len(latencies)))
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(row_numbers, latencies, alpha=0.7, linewidth=0.5)
    ax.set_xlabel("Row Number", fontsize=12)
    ax.set_ylabel("Latency (ns)", fontsize=12)
    ax.set_title("Latency vs Row Number", fontsize=14)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    print(f"Plot saved to {output_path}")
    print(f"Total records: {len(latencies)}")


def main():
    bin_path = Path("build/test.bin")
    
    if not bin_path.exists():
        raise FileNotFoundError(f"Binary file not found: {bin_path}")
    
    print(f"Reading binary file: {bin_path}")
    latencies = load_latencies(bin_path)
    
    output_path = Path("build/test_latency_plot.png")
    plot_latency_vs_row(latencies, output_path)


if __name__ == "__main__":
    main()



