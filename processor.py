"""
The core problem: the same vessel (MMSI) appears across many chunks because
we partitioned by row-order, not by MMSI.  A vessel with 500 pings spread
across a 16M-row file will have its observations scattered across ~10 chunks.

Solution — two-phase Map-Reduce:

  MAP   (parallel, N workers)
  ├─ Receive a chunk of ~50,000 raw rows
  ├─ Parse and validate each field (timestamp, lat, lon, sog, draught …)
  ├─ Group rows by MMSI → {mmsi: [obs, obs, …]}
  └─ Sort each vessel's observations by timestamp
     Return: partial_map  {mmsi: [sorted observations]}

  REDUCE  (sequential, main process — cheap because it is just merging)
  ├─ Receive all N partial maps from all workers
  ├─ For each MMSI: concatenate partial lists from every worker
  └─ Re-sort the merged list by timestamp (each partial list was already
     sorted, so this is an O(k log k) merge where k = total pings per vessel)
     Return: vessel_timelines  {mmsi: [complete chronological observations]}

The vessel_timelines dict is then fed directly into the anomaly detectors
in Task 3 without any further I/O.
"""

import logging
from collections import defaultdict
from datetime import datetime
from multiprocessing import cpu_count
from typing import Any

from partitioner import DEFAULT_CHUNK_SIZE, stream_to_workers

log = logging.getLogger(__name__)

F_TIMESTAMP  = "# Timestamp"       # "dd/mm/yyyy HH:MM:SS"
F_MMSI       = "MMSI"
F_LAT        = "Latitude"
F_LON        = "Longitude"
F_SOG        = "SOG"               # Speed Over Ground, knots
F_COG        = "COG"               # Course Over Ground, degrees
F_HEADING    = "Heading"           # True heading, degrees
F_DRAUGHT    = "Draught"           # Depth in water, metres
F_NAV_STATUS = "Navigational status"
F_SHIP_TYPE  = "Ship type"
F_NAME       = "Name"
F_IMO        = "IMO"

# Timestamp format used by DMA  ("01/06/2025 00:00:00")
TIMESTAMP_FMT = "%d/%m/%Y %H:%M:%S"

# Sentinel float used when a numeric field is missing or unparseable.
MISSING_FLOAT = float("nan")

def _parse_ts(raw: str) -> datetime | None:
# Convert DMA timestamp string to a timezone-naive datetime object.
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, TIMESTAMP_FMT)
    except ValueError:
        return None


def _parse_float(raw: str) -> float:
# Convert a CSV string to float, returning MISSING_FLOAT on failure.
    try:
        return float(raw.strip())
    except (ValueError, AttributeError):
        return MISSING_FLOAT


def _parse_lat(raw: str) -> float:
    """
    Latitude must be in [-90, 90].  The NMEA 'not available' sentinel is
    91.0 (or 0.0 for some transponders).  We reject both extremes.
    """
    v = _parse_float(raw)
    if v != v:          # NaN check (NaN != NaN is always True)
        return MISSING_FLOAT
    if not (-90.0 <= v <= 90.0) or v == 0.0:
        return MISSING_FLOAT
    return v


def _parse_lon(raw: str) -> float:
    """
    Longitude must be in [-180, 180].  The NMEA sentinel is 181.0.
    """
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if not (-180.0 <= v <= 180.0) or v == 0.0:
        return MISSING_FLOAT
    return v


def _parse_sog(raw: str) -> float:
    """
    Speed Over Ground in knots.  Valid range 0-102.2 knots;
    Negative values are corrupt data.
    """
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if v < 0.0 or v >= 102.3:
        return MISSING_FLOAT
    return v


def _parse_draught(raw: str) -> float:
    """
    Valid range 0.1-25.5 m.
    0.0 means 'not available' in the NMEA spec.
    """
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if v <= 0.0 or v > 25.5:
        return MISSING_FLOAT
    return v


def _make_observation(row: dict) -> dict | None:
    """
    Parse one raw CSV row into a clean observation dict.

    """
    ts = _parse_ts(row.get(F_TIMESTAMP, ""))
    if ts is None:
        return None     

    lat = _parse_lat(row.get(F_LAT, ""))
    lon = _parse_lon(row.get(F_LON, ""))

    # accept rows without valid position if they have draught data,
    # Both lat and lon must be valid together, or both rejected.
    if (lat != lat) != (lon != lon):    # one valid, one not 
        lat = lon = MISSING_FLOAT

    return {
        "ts"       : ts,
        "mmsi"     : row[F_MMSI].strip(),
        "lat"      : lat,
        "lon"      : lon,
        "sog"      : _parse_sog(row.get(F_SOG, "")),
        "cog"      : _parse_float(row.get(F_COG, "")),
        "heading"  : _parse_float(row.get(F_HEADING, "")),
        "draught"  : _parse_draught(row.get(F_DRAUGHT, "")),
        "nav"      : row.get(F_NAV_STATUS, "").strip(),
        "ship_type": row.get(F_SHIP_TYPE, "").strip(),
        "name"     : row.get(F_NAME, "").strip(),
        "imo"      : row.get(F_IMO, "").strip(),
    }


# ---------------------------------------------------------------------------
# MAP worker
# ---------------------------------------------------------------------------

def map_worker(chunk: list[dict]) -> dict[str, list[dict]]:
    """
    MAP phase — runs inside a worker process.

    Receives a chunk of ~50,000 raw CSV rows (already filtered by the
    partitioner for valid MMSI).  Returns a partial vessel map:

        { mmsi_string: [observation, observation, …] }

    Each observation list is sorted chronologically by timestamp 

 
    partial_map holds only the observations from this chunk 
    """

    partial_map: dict[str, list[dict]] = defaultdict(list)

    skipped_no_ts = 0

    for row in chunk:
        obs = _make_observation(row)

        if obs is None:
            skipped_no_ts += 1
            continue

        # Group by MMSI 
        partial_map[obs["mmsi"]].append(obs)

    # Sort each vessel's observations within this chunk by timestamp.
    for mmsi in partial_map:
        partial_map[mmsi].sort(key=lambda o: o["ts"])

    return {
        "partial_map"   : dict(partial_map),
        "rows_processed": len(chunk),
        "rows_skipped"  : skipped_no_ts,
        "vessels_seen"  : len(partial_map),
    }


# ---------------------------------------------------------------------------
# REDUCE phase
# ---------------------------------------------------------------------------

def reduce_vessel_timelines(
    map_results: list[dict],
) -> dict[str, list[dict]]:
    """
    REDUCE phase — runs in the main process after all MAP workers finish.

    Takes the list of partial maps (one per chunk) and merges them into a
    single complete vessel timeline dict:

        { mmsi: [obs_t0, obs_t1, obs_t2, …] }   ← full chronological order
    """

    # This dict will hold the complete merged timelines.
    vessel_timelines: dict[str, list[dict]] = defaultdict(list)

    total_rows    = 0
    total_skipped = 0

    for result in map_results:
        partial_map = result["partial_map"]
        total_rows    += result["rows_processed"]
        total_skipped += result["rows_skipped"]

        for mmsi, obs_list in partial_map.items():
            # Extend the master list for this MMSI with the partial list.
            vessel_timelines[mmsi].extend(obs_list)

    log.info(
        "Reduce: merging timelines for %d unique vessels "
        "(total rows: %d, skipped: %d)",
        len(vessel_timelines), total_rows, total_skipped,
    )

    # Final chronological sort across all partial contributions.
    vessels_sorted = 0
    for mmsi in vessel_timelines:
        vessel_timelines[mmsi].sort(key=lambda o: o["ts"])
        vessels_sorted += 1
        if vessels_sorted % 5_000 == 0:
            log.info("  Sorted timelines for %d / %d vessels …",
                     vessels_sorted, len(vessel_timelines))

    log.info("Reduce complete: %d vessel timelines ready.", len(vessel_timelines))

    return dict(vessel_timelines)


# ---------------------------------------------------------------------------
# Top-level pipeline entry point
# ---------------------------------------------------------------------------

def run_map_reduce(
    filepath  : str,
    chunk_size: int       = DEFAULT_CHUNK_SIZE,
    n_workers : int | None = None,
) -> dict[str, list[dict]]:

    import time

    n_workers = n_workers or max(1, cpu_count() - 1)

    log.info("=" * 60)
    log.info("MAP-REDUCE PIPELINE START")
    log.info("  File       : %s", filepath)
    log.info("  Chunk size : %d", chunk_size)
    log.info("  Workers    : %d", n_workers)
    log.info("=" * 60)

    # ── MAP phase ────────────────────────────────────────────────────────
    t_map_start = time.perf_counter()

    map_results = stream_to_workers(
        filepath     = filepath,
        worker_fn    = map_worker,
        chunk_size   = chunk_size,
        n_workers    = n_workers,
    )

    t_map_end = time.perf_counter()
    log.info("MAP phase done in %.2fs", t_map_end - t_map_start)

    # ── REDUCE phase ─────────────────────────────────────────────────────
    t_reduce_start = time.perf_counter()

    vessel_timelines = reduce_vessel_timelines(map_results)

    t_reduce_end = time.perf_counter()
    log.info("REDUCE phase done in %.2fs", t_reduce_end - t_reduce_start)

    # ── Summary ──────────────────────────────────────────────────────────
    total_obs = sum(len(v) for v in vessel_timelines.values())
    ping_counts = sorted(
        (len(v), mmsi) for mmsi, v in vessel_timelines.items()
    )

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("  Total vessels        : %d", len(vessel_timelines))
    log.info("  Total observations   : %d", total_obs)
    log.info("  Avg pings / vessel   : %.1f", total_obs / max(1, len(vessel_timelines)))
    log.info("  Most active vessel   : MMSI %s  (%d pings)",
             ping_counts[-1][1], ping_counts[-1][0])
    log.info("  Wall time (total)    : %.2fs",
             t_reduce_end - t_map_start)
    log.info("=" * 60)

    return vessel_timelines


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import time
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python ais_processor.py <path/to/aisdk-YYYY-MM-DD.csv> [chunk_size]")
        sys.exit(1)

    filepath   = sys.argv[1]
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_CHUNK_SIZE

    timelines = run_map_reduce(filepath, chunk_size=chunk_size)

    # Print first 3 observations of the 3 most active vessels as a sanity check
    ping_counts = sorted(
        (len(v), mmsi) for mmsi, v in timelines.items()
    )
    print("\n── Sample output: top 3 most active vessels ──")
    for n_pings, mmsi in ping_counts[-3:]:
        first = timelines[mmsi][0]
        last  = timelines[mmsi][-1]
        print(
            f"  MMSI {mmsi} | {n_pings} pings | "
            f"{first['ts']} → {last['ts']} | "
            f"name: {first['name'] or 'unknown'}"
        )