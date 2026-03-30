#!/usr/bin/env python3
"""
Task 4 memory profiling entrypoint.

This file is intentionally small so it works cleanly with memory_profiler / mprof.
It runs the same end-to-end workload used in the benchmark.

Examples
--------
# Install once:
#   pip install memory_profiler matplotlib
#   brew install graphviz   (optional, only if your setup needs it)

# Measure memory over time:
mprof run python task4_memory_entry.py aisdk-2025-06-09.csv 50000 8 task4_memory

# Create graph:
mprof plot -o task4_memory/memory_profile.png

# Optional line-by-line peak inspection:
python -m memory_profiler task4_memory_entry.py aisdk-2025-06-09.csv 50000 8 task4_memory
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from processor_updated import run_all_analytics, run_map_reduce

try:
    from memory_profiler import profile
except ImportError:
    # Fallback so the script can still run normally without memory_profiler.
    def profile(func):
        return func


@profile
def main(csv_path: str, chunk_size: int, workers: int, output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timelines = run_map_reduce(filepath=csv_path, chunk_size=chunk_size, n_workers=workers)
    run_all_analytics(timelines, output_dir=output_dir)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python task4_memory_entry.py <path/to/ais.csv> [chunk_size] [workers] [output_dir]")
        sys.exit(1)

    csv_path = sys.argv[1]
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else os.cpu_count() - 1 or 1
    output_dir = sys.argv[4] if len(sys.argv) > 4 else "task4_memory"
    main(csv_path, chunk_size, workers, output_dir)
