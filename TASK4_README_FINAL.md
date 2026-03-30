#Task 4 -- Performance Evaluation & Benchmarking

## Files Overview

-   `task4_benchmark.py`\
    Runs performance experiments:
    -   Speedup (parallel vs sequential)
    -   Chunk-size optimization\
        Saves results as CSV files and plots.
-   `task4_memory_entry.py`\
    Entry point for memory profiling using `memory_profiler` / `mprof`.

------------------------------------------------------------------------

## Installation

Install required dependencies:

``` bash
pip install matplotlib memory_profiler
```

------------------------------------------------------------------------

## Speedup Analysis

Run benchmark with different worker counts:

``` bash
python task4_benchmark.py aisdk-2025-06-09.csv --workers 1 2 4 8 --benchmark-chunk-size 50000 --skip-chunk-optimization
```

### Outputs

-   `task4_results/speedup_results.csv`
-   `task4_results/speedup_wall_time.png`
-   `task4_results/speedup_factor.png`

### Formula Used

Speedup = T1 / Tp

------------------------------------------------------------------------

## 2. Chunk Size Optimization

Run experiments with different chunk sizes:

``` bash
python task4_benchmark.py aisdk-2025-06-09.csv --skip-speedup --chunk-sizes 10000 50000 100000 --chunk-workers 8
```

### Outputs

-   `task4_results/chunk_optimization_results.csv`
-   `task4_results/chunk_optimization.png`

------------------------------------------------------------------------

##  Memory Profiling

Run memory usage analysis:

``` bash
mprof run python task4_memory_entry.py aisdk-2025-06-09.csv 50000 8 task4_memory
mprof plot -o task4_memory/memory_profile.png
```

------------------------------------------------------------------------

# Results Interpretation

## Speedup Analysis

The speedup results show very limited improvement when increasing the
number of workers.

This indicates the system is not CPU-bound but dominated by
multiprocessing overhead, data serialization, and communication costs.

------------------------------------------------------------------------

## Chunk Size Optimization

50,000 rows provided the best performance.

Small chunks caused too much overhead, while large chunks increased
processing cost.

------------------------------------------------------------------------

## Memory Profiling

Memory usage remained stable because the pipeline processes data in
chunks rather than loading the entire dataset.

The memory profiling graph shows an initial spike during worker initialization and chunk buffering, followed by stable oscillations during processing. Memory usage remains bounded and decreases after execution, confirming that data is processed in chunks rather than loaded entirely into memory. The peak usage of approximately 6.3 GB across 8 workers corresponds to less than 1 GB per core, satisfying the assignment’s memory constraint.

------------------------------------------------------------------------

## Anomaly Consistency Across Evaluations

The number of detected anomalies (A, B, C, D) remained consistent across all benchmark runs, regardless of chunk size or number of workers.

This confirms that:
- the pipeline is deterministic
- parallel execution does not affect correctness
- performance tuning does not change analytical results

------------------------------------------------------------------------

## Final Conclusion

The performance evaluation demonstrates that the pipeline successfully handles large-scale AIS data using a **low-memory, streaming-based architecture**. Memory profiling confirms that usage remains bounded and stable, with a peak of approximately **6.3 GB across 8 workers (≈0.79 GB per core)**, satisfying the assignment’s memory constraint.

Chunk-size optimization shows that **50,000 rows per chunk** provides the best balance between task overhead and processing efficiency, highlighting the importance of tuning data partitioning in parallel systems.

However, the speedup analysis reveals **limited performance gains from increasing the number of workers**. This indicates that the system is not CPU-bound, but instead dominated by:
- inter-process communication  
- data serialization (pickling)  
- task dispatch overhead  
- result merging  

As a result, additional parallelism yields diminishing returns.

### Key Insight

> The pipeline is **I/O- and communication-bound rather than computation-bound**, meaning that multiprocessing overhead limits scalability even though the architecture is correct and memory-efficient.
