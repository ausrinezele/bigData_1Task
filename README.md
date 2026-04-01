# Shadow Fleet Detection — AIS Anomaly Analysis

Parallel processing pipeline for detecting illicit maritime behavior (Shadow Fleet) in Danish AIS vessel tracking data.

**Dataset:** [Danish Maritime Authority AIS Data](http://aisdata.ais.dk/) — `aisdk-2025-06-09.csv and aisdk-2025-06-10.csv` (~2 GB, not included in repo)

## Architecture

The pipeline processes gigabyte-scale CSV files under strict memory constraints using a streaming Map-Reduce design:

1. **Partitioner** (`partitioner.py`) — streams the CSV line-by-line via `csv.DictReader` + generators, filters dirty MMSI data, batches into 50K-row chunks, and dispatches to worker processes via `multiprocessing.Pool.imap_unordered`.

2. **Processor** (`processor_updated.py`) — MAP phase parses and groups rows into partial per-MMSI timelines in parallel workers. REDUCE phase merges and sorts into complete vessel timelines.

3. **Anomaly Detection** (`anomalies_updated.py`) — detects four anomaly types:
   - **A (Going Dark):** AIS gaps > 4 hours where vessel kept moving
   - **B (Loitering/Transfers):** Two vessels within 500m, SOG < 1 kn, > 2 hours
   - **C (Draft Change at Sea):** Draught Δ > 5% during AIS blackout > 2 hours
   - **D (Teleportation/Cloning):** Same MMSI at locations requiring > 60 knot speed

4. **DFSI Calculation:** `(Max Gap Hours / 2) + (Total Impossible Distance NM / 10) + (C × 15)`

## Quick Start

```bash
pip install -r requirements.txt

# Run full pipeline (Tasks 1-3)
python processor_updated.py aisdk-2025-06-09.csv 50000 8 results

# Run benchmarks (Task 4 — speedup + chunk optimization)
python task4_benchmark.py aisdk-2025-06-09.csv \
    --workers 1 2 4 8 \
    --chunk-sizes 10000 50000 100000 \
    --output-dir task4_results

# Run memory profiling (Task 4)
mprof run python task4_memory_entry.py aisdk-2025-06-09.csv 50000 8 task4_memory
mprof plot -o task4_memory/memory_profile.png
```

## Output

The pipeline writes to the specified output directory:

- `anomaly_a.json` — Going Dark events with coordinates
- `anomaly_b.json` — Loitering/Transfer pair-events
- `anomaly_c.json` — Draft Change events
- `anomaly_d.json` — Teleportation events
- `dfsi_ranked.json` / `dfsi_ranked.csv` — All flagged vessels ranked by DFSI score

## Files

| File | Description |
|------|-------------|
| `partitioner.py` | Low-memory streaming partitioner with dirty data filtering |
| `processor_updated.py` | Map-Reduce pipeline + anomaly orchestration (main entry point) |
| `anomalies_updated.py` | Anomaly detectors A, B, C, D and DFSI calculation |
| `task4_benchmark.py` | Speedup and chunk-size benchmarking |
| `task4_memory_entry.py` | Entry point for `mprof` memory profiling |
