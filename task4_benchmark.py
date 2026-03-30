#!/usr/bin/env python3
"""
Task 4 benchmark runner for the Maritime Shadow Fleet assignment.

What this script does
---------------------
1. Runs the full pipeline (MapReduce + Task 3 analytics) with different worker counts.
2. Computes speedup relative to the 1-worker baseline.
3. Runs chunk-size experiments to study chunk optimization.
4. Writes CSV summaries and generates PNG graphs for the presentation.

Examples
--------
# Full benchmark set: sequential vs parallel + chunk optimization
python task4_benchmark.py aisdk-2025-06-09.csv \
    --workers 1 2 4 8 \
    --chunk-sizes 10000 50000 100000 \
    --benchmark-chunk-size 50000 \
    --output-dir task4_results

# Only speedup analysis
python task4_benchmark.py aisdk-2025-06-09.csv \
    --workers 1 2 4 8 \
    --skip-chunk-optimization

# Only chunk optimization at a fixed worker count
python task4_benchmark.py aisdk-2025-06-09.csv \
    --skip-speedup \
    --chunk-sizes 10000 50000 100000 \
    --chunk-workers 8

Memory profiling
----------------
Use the separate entrypoint script:
    mprof run python task4_memory_entry.py aisdk-2025-06-09.csv 50000 8 task4_memory
    mprof plot -o task4_memory/memory_profile.png
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import shutil
import statistics
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt

from processor_updated import run_all_analytics, run_map_reduce


@dataclass
class BenchmarkRow:
    experiment: str
    dataset: str
    chunk_size: int
    workers: int
    wall_time_seconds: float
    speedup_vs_1_worker: float | None
    efficiency_vs_1_worker: float | None
    output_dir: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_rmtree(path: str | Path) -> None:
    p = Path(path)
    if p.exists() and p.is_dir():
        shutil.rmtree(p)


def write_csv(path: str | Path, rows: list[BenchmarkRow]) -> None:
    path = Path(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "experiment",
                "dataset",
                "chunk_size",
                "workers",
                "wall_time_seconds",
                "speedup_vs_1_worker",
                "efficiency_vs_1_worker",
                "output_dir",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def timed_full_run(csv_path: str, chunk_size: int, workers: int, output_dir: Path) -> float:
    """
    Time the full submission-relevant pipeline:
      1. MapReduce timeline construction
      2. Task 3 anomaly analytics
      3. DFSI ranking + file outputs

    This is the fairest benchmark for the assignment because it measures the
    complete end-to-end workload, not only the partitioning stage.
    """
    safe_rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    timelines = run_map_reduce(filepath=csv_path, chunk_size=chunk_size, n_workers=workers)
    run_all_analytics(timelines, output_dir=str(output_dir))
    t1 = time.perf_counter()
    return t1 - t0


# ---------------------------------------------------------------------------
# Experiment runners
# ---------------------------------------------------------------------------

def run_speedup_experiment(
    csv_path: str,
    workers_list: list[int],
    chunk_size: int,
    output_root: Path,
) -> list[BenchmarkRow]:
    if 1 not in workers_list:
        workers_list = [1] + workers_list
    workers_list = sorted(set(workers_list))

    rows: list[BenchmarkRow] = []
    baseline_time: float | None = None

    print("\n=== Speedup experiment ===")
    print(f"Dataset      : {csv_path}")
    print(f"Chunk size   : {chunk_size}")
    print(f"Workers list : {workers_list}")

    for workers in workers_list:
        run_dir = output_root / f"speedup_w{workers}_c{chunk_size}"
        wall = timed_full_run(csv_path, chunk_size, workers, run_dir)

        if workers == 1:
            baseline_time = wall

        speedup = (baseline_time / wall) if (baseline_time is not None and wall > 0) else None
        efficiency = (speedup / workers) if (speedup is not None and workers > 0) else None

        row = BenchmarkRow(
            experiment="speedup",
            dataset=os.path.basename(csv_path),
            chunk_size=chunk_size,
            workers=workers,
            wall_time_seconds=round(wall, 6),
            speedup_vs_1_worker=round(speedup, 6) if speedup is not None else None,
            efficiency_vs_1_worker=round(efficiency, 6) if efficiency is not None else None,
            output_dir=str(run_dir),
        )
        rows.append(row)

        print(
            f"workers={workers:>2} | wall={wall:>9.2f}s"
            + (f" | speedup={speedup:>6.2f}x | efficiency={efficiency:>5.2f}" if speedup is not None else "")
        )

    return rows


def run_chunk_optimization_experiment(
    csv_path: str,
    chunk_sizes: list[int],
    workers: int,
    output_root: Path,
) -> list[BenchmarkRow]:
    chunk_sizes = sorted(set(chunk_sizes))
    rows: list[BenchmarkRow] = []

    print("\n=== Chunk-size optimization experiment ===")
    print(f"Dataset      : {csv_path}")
    print(f"Workers      : {workers}")
    print(f"Chunk sizes  : {chunk_sizes}")

    baseline_time: float | None = None
    baseline_chunk: int | None = None

    for chunk_size in chunk_sizes:
        run_dir = output_root / f"chunkopt_w{workers}_c{chunk_size}"
        wall = timed_full_run(csv_path, chunk_size, workers, run_dir)

        if baseline_time is None:
            baseline_time = wall
            baseline_chunk = chunk_size

        # For chunk optimization, keep the same CSV schema for convenience.
        # Speedup here means relative to the first tested chunk size.
        speedup = (baseline_time / wall) if wall > 0 else None
        efficiency = None

        row = BenchmarkRow(
            experiment="chunk_optimization",
            dataset=os.path.basename(csv_path),
            chunk_size=chunk_size,
            workers=workers,
            wall_time_seconds=round(wall, 6),
            speedup_vs_1_worker=round(speedup, 6) if speedup is not None else None,
            efficiency_vs_1_worker=efficiency,
            output_dir=str(run_dir),
        )
        rows.append(row)

        baseline_label = f" (baseline chunk={baseline_chunk})" if chunk_size == baseline_chunk else ""
        print(
            f"chunk={chunk_size:>7,} | workers={workers:>2} | wall={wall:>9.2f}s"
            + (f" | relative_speed={speedup:>6.2f}x" if speedup is not None else "")
            + baseline_label
        )

    return rows


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_speedup(rows: list[BenchmarkRow], output_dir: Path) -> None:
    speedup_rows = [r for r in rows if r.experiment == "speedup"]
    if not speedup_rows:
        return

    speedup_rows.sort(key=lambda r: r.workers)
    workers = [r.workers for r in speedup_rows]
    wall_times = [r.wall_time_seconds for r in speedup_rows]
    speedups = [r.speedup_vs_1_worker or 0.0 for r in speedup_rows]

    # Wall-time chart
    plt.figure(figsize=(8, 5))
    plt.plot(workers, wall_times, marker="o")
    plt.xlabel("Number of workers")
    plt.ylabel("Wall time (seconds)")
    plt.title("Task 4: Execution Time vs Worker Count")
    plt.grid(True, alpha=0.3)
    plt.xticks(workers)
    plt.tight_layout()
    plt.savefig(output_dir / "speedup_wall_time.png", dpi=180)
    plt.close()

    # Speedup chart with ideal line
    plt.figure(figsize=(8, 5))
    plt.plot(workers, speedups, marker="o", label="Measured speedup")
    plt.plot(workers, workers, linestyle="--", marker="o", label="Ideal linear speedup")
    plt.xlabel("Number of workers")
    plt.ylabel("Speedup vs 1 worker")
    plt.title("Task 4: Parallel Speedup")
    plt.grid(True, alpha=0.3)
    plt.xticks(workers)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_dir / "speedup_factor.png", dpi=180)
    plt.close()


def plot_chunk_optimization(rows: list[BenchmarkRow], output_dir: Path) -> None:
    chunk_rows = [r for r in rows if r.experiment == "chunk_optimization"]
    if not chunk_rows:
        return

    chunk_rows.sort(key=lambda r: r.chunk_size)
    chunks = [r.chunk_size for r in chunk_rows]
    wall_times = [r.wall_time_seconds for r in chunk_rows]

    plt.figure(figsize=(8, 5))
    plt.plot(chunks, wall_times, marker="o")
    plt.xlabel("Chunk size (rows)")
    plt.ylabel("Wall time (seconds)")
    plt.title("Task 4: Chunk Size Optimization")
    plt.grid(True, alpha=0.3)
    plt.xticks(chunks)
    plt.tight_layout()
    plt.savefig(output_dir / "chunk_optimization.png", dpi=180)
    plt.close()


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Task 4 benchmark runner")
    parser.add_argument("csv_path", help="Path to AIS CSV file")
    parser.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8],
        help="Worker counts for speedup analysis (default: 1 2 4 8)",
    )
    parser.add_argument(
        "--chunk-sizes",
        type=int,
        nargs="+",
        default=[10000, 50000, 100000],
        help="Chunk sizes for chunk optimization (default: 10000 50000 100000)",
    )
    parser.add_argument(
        "--benchmark-chunk-size",
        type=int,
        default=50000,
        help="Chunk size used during the speedup experiment (default: 50000)",
    )
    parser.add_argument(
        "--chunk-workers",
        type=int,
        default=8,
        help="Worker count used during chunk optimization (default: 8)",
    )
    parser.add_argument(
        "--output-dir",
        default="task4_results",
        help="Directory for CSVs, plots, and per-run outputs (default: task4_results)",
    )
    parser.add_argument(
        "--skip-speedup",
        action="store_true",
        help="Skip the sequential-vs-parallel speedup experiment",
    )
    parser.add_argument(
        "--skip-chunk-optimization",
        action="store_true",
        help="Skip the chunk-size experiment",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = ensure_dir(args.output_dir)

    all_rows: list[BenchmarkRow] = []

    if not args.skip_speedup:
        speedup_rows = run_speedup_experiment(
            csv_path=args.csv_path,
            workers_list=args.workers,
            chunk_size=args.benchmark_chunk_size,
            output_root=output_root,
        )
        all_rows.extend(speedup_rows)
        write_csv(output_root / "speedup_results.csv", speedup_rows)
        plot_speedup(speedup_rows, output_root)

    if not args.skip_chunk_optimization:
        chunk_rows = run_chunk_optimization_experiment(
            csv_path=args.csv_path,
            chunk_sizes=args.chunk_sizes,
            workers=args.chunk_workers,
            output_root=output_root,
        )
        all_rows.extend(chunk_rows)
        write_csv(output_root / "chunk_optimization_results.csv", chunk_rows)
        plot_chunk_optimization(chunk_rows, output_root)

    if all_rows:
        write_csv(output_root / "task4_all_results.csv", all_rows)

    print("\n=== Task 4 outputs written ===")
    print(f"Directory: {output_root.resolve()}")
    print("Files you can use in the report/slides:")
    print("  - speedup_results.csv")
    print("  - chunk_optimization_results.csv")
    print("  - task4_all_results.csv")
    print("  - speedup_wall_time.png")
    print("  - speedup_factor.png")
    print("  - chunk_optimization.png")
    print("  - per-run folders with anomaly and DFSI outputs")


if __name__ == "__main__":
    main()
