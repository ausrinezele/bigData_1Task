"""
Parallel AIS processor for Task 2 + Task 3.

What this script does:
1. Uses the existing partitioner unchanged.
2. Builds full vessel timelines with a parallel map / sequential reduce flow.
3. Runs anomaly detectors A, B, C, D.
4. Calculates DFSI for every flagged vessel using the assignment formula.
5. Writes ready-to-submit result files to an output directory.

Run example:
    python processor_updated.py /path/to/aisdk-YYYY-MM-DD.csv

Optional:
    python processor_updated.py /path/to/file.csv 50000 8 output_dir
"""

import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from multiprocessing import cpu_count
from typing import Any

from partitioner import DEFAULT_CHUNK_SIZE, stream_to_workers
from anomalies_updated import (
    calculate_dfsi,
    detect_anomaly_a_going_dark,
    detect_anomaly_b_loitering_transfer,
    detect_anomaly_c_draft_change,
    detect_anomaly_d_teleportation,
    summarize_anomaly_a,
    summarize_anomaly_b,
    summarize_anomaly_c,
    summarize_anomaly_d,
    summarize_dfsi,
)

log = logging.getLogger(__name__)

F_TIMESTAMP = "# Timestamp"
F_MMSI = "MMSI"
F_LAT = "Latitude"
F_LON = "Longitude"
F_SOG = "SOG"
F_COG = "COG"
F_HEADING = "Heading"
F_DRAUGHT = "Draught"
F_NAV_STATUS = "Navigational status"
F_SHIP_TYPE = "Ship type"
F_NAME = "Name"
F_IMO = "IMO"

TIMESTAMP_FMT = "%d/%m/%Y %H:%M:%S"
MISSING_FLOAT = float("nan")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_ts(raw: str) -> datetime | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, TIMESTAMP_FMT)
    except ValueError:
        return None



def _parse_float(raw: str) -> float:
    try:
        return float(raw.strip())
    except (ValueError, AttributeError):
        return MISSING_FLOAT



def _parse_lat(raw: str) -> float:
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if not (-90.0 <= v <= 90.0) or v == 0.0:
        return MISSING_FLOAT
    return v



def _parse_lon(raw: str) -> float:
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if not (-180.0 <= v <= 180.0) or v == 0.0:
        return MISSING_FLOAT
    return v



def _parse_sog(raw: str) -> float:
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if v < 0.0 or v >= 102.3:
        return MISSING_FLOAT
    return v



def _parse_draught(raw: str) -> float:
    v = _parse_float(raw)
    if v != v:
        return MISSING_FLOAT
    if v <= 0.0 or v > 25.5:
        return MISSING_FLOAT
    return v



def _make_observation(row: dict) -> dict | None:
    ts = _parse_ts(row.get(F_TIMESTAMP, ""))
    if ts is None:
        return None

    lat = _parse_lat(row.get(F_LAT, ""))
    lon = _parse_lon(row.get(F_LON, ""))

    if (lat != lat) != (lon != lon):
        lat = lon = MISSING_FLOAT

    return {
        "ts": ts,
        "mmsi": row[F_MMSI].strip(),
        "lat": lat,
        "lon": lon,
        "sog": _parse_sog(row.get(F_SOG, "")),
        "cog": _parse_float(row.get(F_COG, "")),
        "heading": _parse_float(row.get(F_HEADING, "")),
        "draught": _parse_draught(row.get(F_DRAUGHT, "")),
        "nav": row.get(F_NAV_STATUS, "").strip(),
        "ship_type": row.get(F_SHIP_TYPE, "").strip(),
        "name": row.get(F_NAME, "").strip(),
        "imo": row.get(F_IMO, "").strip(),
    }


# ---------------------------------------------------------------------------
# MAP worker
# ---------------------------------------------------------------------------

def map_worker(chunk: list[dict]) -> dict[str, Any]:
    partial_map: dict[str, list[dict]] = defaultdict(list)
    skipped_no_ts = 0

    for row in chunk:
        obs = _make_observation(row)
        if obs is None:
            skipped_no_ts += 1
            continue
        partial_map[obs["mmsi"]].append(obs)

    for mmsi in partial_map:
        partial_map[mmsi].sort(key=lambda o: o["ts"])

    return {
        "partial_map": dict(partial_map),
        "rows_processed": len(chunk),
        "rows_skipped": skipped_no_ts,
        "vessels_seen": len(partial_map),
    }


# ---------------------------------------------------------------------------
# REDUCE phase
# ---------------------------------------------------------------------------

def reduce_vessel_timelines(map_results: list[dict]) -> dict[str, list[dict]]:
    vessel_timelines: dict[str, list[dict]] = defaultdict(list)

    total_rows = 0
    total_skipped = 0

    for result in map_results:
        partial_map = result["partial_map"]
        total_rows += result["rows_processed"]
        total_skipped += result["rows_skipped"]

        for mmsi, obs_list in partial_map.items():
            vessel_timelines[mmsi].extend(obs_list)

    log.info(
        "Reduce: merging timelines for %d unique vessels (rows=%d, skipped=%d)",
        len(vessel_timelines),
        total_rows,
        total_skipped,
    )

    for idx, mmsi in enumerate(vessel_timelines, start=1):
        vessel_timelines[mmsi].sort(key=lambda o: o["ts"])
        if idx % 5000 == 0:
            log.info("Sorted %d / %d vessel timelines", idx, len(vessel_timelines))

    log.info("Reduce complete: %d vessel timelines ready", len(vessel_timelines))
    return dict(vessel_timelines)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_map_reduce(filepath: str, chunk_size: int = DEFAULT_CHUNK_SIZE, n_workers: int | None = None) -> dict[str, list[dict]]:
    import time

    n_workers = n_workers or max(1, cpu_count() - 1)

    log.info("=" * 70)
    log.info("MAP-REDUCE PIPELINE START")
    log.info("File       : %s", filepath)
    log.info("Chunk size : %d", chunk_size)
    log.info("Workers    : %d", n_workers)
    log.info("=" * 70)

    t0 = time.perf_counter()
    map_results = stream_to_workers(
        filepath=filepath,
        worker_fn=map_worker,
        chunk_size=chunk_size,
        n_workers=n_workers,
    )
    t1 = time.perf_counter()
    log.info("MAP phase done in %.2fs", t1 - t0)

    vessel_timelines = reduce_vessel_timelines(map_results)
    t2 = time.perf_counter()
    log.info("REDUCE phase done in %.2fs", t2 - t1)
    log.info("TOTAL pipeline time %.2fs", t2 - t0)

    return vessel_timelines


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def ensure_output_dir(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)



def _json_default(obj: Any):
    if isinstance(obj, datetime):
        return obj.isoformat(sep=" ")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")



def write_json(filepath: str, data: Any) -> None:
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, default=_json_default)



def write_flagged_vessels_csv(filepath: str, ranked_dfsi: list[dict[str, Any]]) -> None:
    fieldnames = [
        "rank",
        "mmsi",
        "name",
        "dfsi",
        "max_gap_hours",
        "total_impossible_distance_nm",
        "draft_change_count",
        "anomaly_a_count",
        "anomaly_c_count",
        "anomaly_d_count",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for idx, row in enumerate(ranked_dfsi, start=1):
            writer.writerow({"rank": idx, **row})



def print_top_anomaly_a(anomaly_a: dict[str, list[dict]], limit: int = 5) -> None:
    summary = summarize_anomaly_a(anomaly_a)
    total_events = sum(len(v) for v in anomaly_a.values())
    print("\n── Anomaly A: Going Dark ──")
    print(f"Vessels flagged: {len(anomaly_a)}")
    print(f"Total A-events : {total_events}")
    if not summary:
        print("No Anomaly A events found.")
        return
    print("\nTop vessels:")
    for count, max_gap, max_dist, mmsi in summary[:limit]:
        first_event = anomaly_a[mmsi][0]
        print(
            f"  MMSI {mmsi} | A-events={count} | max_gap={max_gap:.2f}h | "
            f"max_distance={max_dist:.2f}nm | name={first_event.get('start_name') or 'unknown'}"
        )



def print_top_anomaly_b(anomaly_b: list[dict], limit: int = 5) -> None:
    summary = summarize_anomaly_b(anomaly_b)
    print("\n── Anomaly B: Loitering & Transfers ──")
    print(f"Pair-events flagged: {len(anomaly_b)}")
    if not summary:
        print("No Anomaly B events found.")
        return
    print("\nTop pair-events:")
    for duration_h, avg_dist_m, m1, m2 in summary[:limit]:
        event = next(
            e for e in anomaly_b
            if e["mmsi_1"] == m1 and e["mmsi_2"] == m2 and e["duration_hours"] == duration_h
        )
        print(
            f"  MMSI {m1} ↔ MMSI {m2} | duration={duration_h:.2f}h | "
            f"avg_distance={avg_dist_m:.2f}m | names=({event.get('name_1') or 'unknown'}) / ({event.get('name_2') or 'unknown'})"
        )



def print_top_anomaly_c(anomaly_c: dict[str, list[dict]], debug_c: dict[str, int] | None, limit: int = 5) -> None:
    summary = summarize_anomaly_c(anomaly_c)
    total_events = sum(len(v) for v in anomaly_c.values())
    print("\n── Anomaly C: Draft Changes at Sea ──")
    print(f"Vessels flagged: {len(anomaly_c)}")
    print(f"Total C-events : {total_events}")
    if summary:
        print("\nTop vessels:")
        for count, max_pct, max_gap, mmsi in summary[:limit]:
            first_event = anomaly_c[mmsi][0]
            print(
                f"  MMSI {mmsi} | C-events={count} | max_draft_change={max_pct:.2f}% | "
                f"max_gap={max_gap:.2f}h | name={first_event.get('start_name') or 'unknown'}"
            )
    else:
        print("No Anomaly C events found.")

    if debug_c is not None:
        print("\n[DEBUG C]")
        for key, value in debug_c.items():
            print(f"{key:24}: {value}")



def print_top_anomaly_d(anomaly_d: dict[str, list[dict]], limit: int = 5) -> None:
    summary = summarize_anomaly_d(anomaly_d)
    total_events = sum(len(v) for v in anomaly_d.values())
    print("\n── Anomaly D: Identity Cloning / Teleportation ──")
    print(f"Vessels flagged: {len(anomaly_d)}")
    print(f"Total D-events : {total_events}")
    if not summary:
        print("No Anomaly D events found.")
        return
    print("\nTop vessels:")
    for count, max_speed, max_dist, mmsi in summary[:limit]:
        first_event = anomaly_d[mmsi][0]
        print(
            f"  MMSI {mmsi} | D-events={count} | max_speed={max_speed:.2f}kn | "
            f"max_distance={max_dist:.2f}nm | name={first_event.get('start_name') or 'unknown'}"
        )



def print_top_dfsi(ranked_dfsi: list[dict[str, Any]], limit: int = 10) -> None:
    print("\n── DFSI Ranking ──")
    print("Formula: DFSI = (Max Gap Hours / 2) + (Total Impossible Distance Jump NM / 10) + (C * 15)")
    if not ranked_dfsi:
        print("No flagged vessels for DFSI.")
        return
    print("\nTop vessels by DFSI:")
    for idx, row in enumerate(ranked_dfsi[:limit], start=1):
        print(
            f"  #{idx} MMSI {row['mmsi']} | DFSI={row['dfsi']:.3f} | "
            f"max_gap={row['max_gap_hours']:.2f}h | impossible_jump_total={row['total_impossible_distance_nm']:.2f}nm | "
            f"C={row['draft_change_count']} | A={row['anomaly_a_count']} | D={row['anomaly_d_count']} | "
            f"name={row['name'] or 'unknown'}"
        )


# ---------------------------------------------------------------------------
# Main task 3 runner
# ---------------------------------------------------------------------------

def run_all_analytics(
    timelines: dict[str, list[dict]],
    output_dir: str,
) -> dict[str, Any]:
    ensure_output_dir(output_dir)

    anomaly_a = detect_anomaly_a_going_dark(
        timelines,
        min_gap_hours=4.0,
        min_distance_nm=2.0,
        min_implied_speed_knots=0.5,
    )

    anomaly_c, debug_c = detect_anomaly_c_draft_change(
        timelines,
        min_gap_hours=2.0,
        min_draft_change_ratio=0.05,
        min_distance_nm=0.5,
        debug=True,
    )

    anomaly_d = detect_anomaly_d_teleportation(
        timelines,
        min_speed_knots=60.0,
        min_gap_seconds=60.0,
    )

    anomaly_b = detect_anomaly_b_loitering_transfer(
        timelines,
        max_distance_m=500.0,  # assignment threshold kept
        max_sog_knots=1.0,  # assignment threshold kept
        min_duration_hours=2.0,  # assignment threshold kept
        bucket_minutes=10,
        exclude_nav_statuses={"Moored", "At anchor"},  # CHANGED: remove obvious harbor false positives
        max_avg_distance_m=250.0,  # CHANGED: pair must be much closer on average
        max_event_distance_m=350.0,  # CHANGED: reject pairs living near 500m edge
        allow_one_missing_bucket=True,  # CHANGED: tolerate one missing AIS bucket
    )

    dfsi_by_vessel = calculate_dfsi(anomaly_a, anomaly_c, anomaly_d)
    ranked_dfsi = summarize_dfsi(dfsi_by_vessel)

    write_json(os.path.join(output_dir, "anomaly_a.json"), anomaly_a)
    write_json(os.path.join(output_dir, "anomaly_b.json"), anomaly_b)
    write_json(os.path.join(output_dir, "anomaly_c.json"), anomaly_c)
    write_json(os.path.join(output_dir, "anomaly_d.json"), anomaly_d)
    write_json(os.path.join(output_dir, "dfsi_ranked.json"), ranked_dfsi)
    write_flagged_vessels_csv(os.path.join(output_dir, "dfsi_ranked.csv"), ranked_dfsi)

    print_top_anomaly_a(anomaly_a)
    print_top_anomaly_b(anomaly_b)
    print_top_anomaly_c(anomaly_c, debug_c)
    print_top_anomaly_d(anomaly_d)
    print_top_dfsi(ranked_dfsi)

    return {
        "anomaly_a": anomaly_a,
        "anomaly_b": anomaly_b,
        "anomaly_c": anomaly_c,
        "anomaly_d": anomaly_d,
        "debug_c": debug_c,
        "dfsi_by_vessel": dfsi_by_vessel,
        "ranked_dfsi": ranked_dfsi,
    }


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python processor_updated.py <path/to/ais.csv> [chunk_size] [n_workers] [output_dir]")
        sys.exit(1)

    filepath = sys.argv[1]
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_CHUNK_SIZE
    n_workers = int(sys.argv[3]) if len(sys.argv) > 3 else max(1, cpu_count() - 1)
    output_dir = sys.argv[4] if len(sys.argv) > 4 else "results"

    timelines = run_map_reduce(filepath=filepath, chunk_size=chunk_size, n_workers=n_workers)
    run_all_analytics(timelines, output_dir=output_dir)
