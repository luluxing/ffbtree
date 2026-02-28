from __future__ import annotations

import os
from pathlib import Path
from typing import List, Sequence, Tuple

import argparse
import matplotlib.pyplot as plt


def find_err_files(file_prefix: str) -> List[Path]:
    """
    Return all .err files in the prefix directory whose names contain the prefix.

    Args:
        file_prefix: Path-like prefix used to narrow the directory and match names.

    """
    prefix_path = Path(file_prefix)
    directory = prefix_path.parent if str(prefix_path.parent) != "" else Path(".")
    if not directory.exists():
        return []

    needle = prefix_path.name
    matches: List[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue
                if not entry.name.endswith(".err"):
                    continue
                if needle and needle not in entry.name:
                    continue
                matches.append(directory / entry.name)
    except FileNotFoundError:
        return []

    matches.sort()
    print(matches)
    return matches


def load_restart_counts(path: Path) -> List[int]:
    """Return restart counts from the given file."""
    counts: List[int] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line:
                counts.append(int(line))
    return counts


def plot_restart_counts(datasets: Sequence[Tuple[Path, List[int]]], output: Path | None) -> None:
    """Plot restart count data and save or display the figure."""
    fig, ax = plt.subplots()
    for path, counts in datasets:
        if not counts:
            continue
        xs = range(1, len(counts) + 1)
        name = path.name
        lower_name = name.lower()
        if "btreeolc" in lower_name:
            label = "btreeolc"
        elif "fftreeolc" in lower_name:
            label = "fftreeolc"
        else:
            label = name
        ax.plot(xs, counts, label=label)

    ax.set_xlabel("Row")
    ax.set_ylabel("Restarts")
    ax.set_title("Restart Counts")
    ax.legend(loc="upper left")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)

    if output:
        fig.savefig(output)
    else:
        plt.show()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot restart counts from .err files sharing a prefix.")
    parser.add_argument("prefix", help="File prefix used to locate .err files.")
    parser.add_argument("-o", "--output", type=Path, help="Optional path to save the figure instead of showing it.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    files = find_err_files(args.prefix)
    if len(files) < 2:
        raise SystemExit(f"Expected at least two .err files for prefix '{args.prefix}', found {len(files)}.")

    datasets = [(path, load_restart_counts(path)) for path in files]
    plot_restart_counts(datasets, args.output)


if __name__ == "__main__":
    main()
