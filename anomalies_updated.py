import math
from collections import defaultdict
from datetime import datetime
from itertools import combinations
from typing import Any

EARTH_RADIUS_KM = 6371.0088
KM_TO_NM = 0.5399568
NM_TO_METERS = 1852.0


def is_missing_float(x: float) -> bool:
    return isinstance(x, float) and math.isnan(x)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_KM * c


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return haversine_km(lat1, lon1, lat2, lon2) * KM_TO_NM


# ---------------------------------------------------------------------------
# Anomaly A: Going Dark
# ---------------------------------------------------------------------------

def detect_anomaly_a_going_dark(
    vessel_timelines: dict[str, list[dict]],
    min_gap_hours: float = 4.0,
    min_distance_nm: float = 2.0,
    min_implied_speed_knots: float = 0.5,
) -> dict[str, list[dict[str, Any]]]:
    """
    Detect AIS gaps > min_gap_hours where the vessel likely kept moving.
    """
    anomalies_by_vessel: dict[str, list[dict[str, Any]]] = {}

    for mmsi, obs_list in vessel_timelines.items():
        if len(obs_list) < 2:
            continue

        vessel_anomalies: list[dict[str, Any]] = []

        for prev_obs, curr_obs in zip(obs_list, obs_list[1:]):
            ts1 = prev_obs["ts"]
            ts2 = curr_obs["ts"]

            if not isinstance(ts1, datetime) or not isinstance(ts2, datetime):
                continue

            gap_hours = (ts2 - ts1).total_seconds() / 3600.0
            if gap_hours <= min_gap_hours:
                continue

            lat1 = prev_obs["lat"]
            lon1 = prev_obs["lon"]
            lat2 = curr_obs["lat"]
            lon2 = curr_obs["lon"]

            if (
                is_missing_float(lat1) or is_missing_float(lon1)
                or is_missing_float(lat2) or is_missing_float(lon2)
            ):
                continue

            distance_nm = haversine_nm(lat1, lon1, lat2, lon2)
            implied_speed_knots = distance_nm / gap_hours if gap_hours > 0 else 0.0

            if distance_nm >= min_distance_nm and implied_speed_knots >= min_implied_speed_knots:
                vessel_anomalies.append(
                    {
                        "type": "A",
                        "gap_start_ts": ts1,
                        "gap_end_ts": ts2,
                        "gap_hours": round(gap_hours, 3),
                        "start_lat": lat1,
                        "start_lon": lon1,
                        "end_lat": lat2,
                        "end_lon": lon2,
                        "distance_nm": round(distance_nm, 3),
                        "implied_speed_knots": round(implied_speed_knots, 3),
                        "start_name": prev_obs.get("name", ""),
                        "end_name": curr_obs.get("name", ""),
                    }
                )

        if vessel_anomalies:
            anomalies_by_vessel[mmsi] = vessel_anomalies

    return anomalies_by_vessel


def summarize_anomaly_a(anomalies_by_vessel: dict[str, list[dict[str, Any]]]) -> list[tuple]:
    summary = []
    for mmsi, events in anomalies_by_vessel.items():
        count = len(events)
        max_gap = max(e["gap_hours"] for e in events)
        max_dist = max(e["distance_nm"] for e in events)
        summary.append((count, max_gap, max_dist, mmsi))
    summary.sort(reverse=True)
    return summary


# ---------------------------------------------------------------------------
# Anomaly D: Identity Cloning / Teleportation
# ---------------------------------------------------------------------------

def detect_anomaly_d_teleportation(
    vessel_timelines: dict[str, list[dict]],
    min_speed_knots: float = 60.0,
    min_gap_seconds: float = 60.0,
) -> dict[str, list[dict[str, Any]]]:
    """
    Detect impossible travel for the same MMSI.
    """
    anomalies_by_vessel: dict[str, list[dict[str, Any]]] = {}

    for mmsi, obs_list in vessel_timelines.items():
        if len(obs_list) < 2:
            continue

        vessel_anomalies: list[dict[str, Any]] = []

        for prev_obs, curr_obs in zip(obs_list, obs_list[1:]):
            ts1 = prev_obs["ts"]
            ts2 = curr_obs["ts"]

            if not isinstance(ts1, datetime) or not isinstance(ts2, datetime):
                continue

            gap_seconds = (ts2 - ts1).total_seconds()
            if gap_seconds <= min_gap_seconds:
                continue

            gap_hours = gap_seconds / 3600.0

            lat1 = prev_obs["lat"]
            lon1 = prev_obs["lon"]
            lat2 = curr_obs["lat"]
            lon2 = curr_obs["lon"]

            if (
                is_missing_float(lat1) or is_missing_float(lon1)
                or is_missing_float(lat2) or is_missing_float(lon2)
            ):
                continue

            distance_nm = haversine_nm(lat1, lon1, lat2, lon2)
            implied_speed_knots = distance_nm / gap_hours if gap_hours > 0 else 0.0

            if implied_speed_knots > min_speed_knots and distance_nm >= 1.0:
                vessel_anomalies.append(
                    {
                        "type": "D",
                        "start_ts": ts1,
                        "end_ts": ts2,
                        "gap_hours": round(gap_hours, 6),
                        "start_lat": lat1,
                        "start_lon": lon1,
                        "end_lat": lat2,
                        "end_lon": lon2,
                        "distance_nm": round(distance_nm, 3),
                        "implied_speed_knots": round(implied_speed_knots, 3),
                        "start_name": prev_obs.get("name", ""),
                        "end_name": curr_obs.get("name", ""),
                    }
                )

        if vessel_anomalies:
            anomalies_by_vessel[mmsi] = vessel_anomalies

    return anomalies_by_vessel


def summarize_anomaly_d(anomalies_by_vessel: dict[str, list[dict[str, Any]]]) -> list[tuple]:
    summary = []
    for mmsi, events in anomalies_by_vessel.items():
        count = len(events)
        max_speed = max(e["implied_speed_knots"] for e in events)
        max_dist = max(e["distance_nm"] for e in events)
        summary.append((count, max_speed, max_dist, mmsi))
    summary.sort(reverse=True)
    return summary


# ---------------------------------------------------------------------------
# Anomaly C: Draft Changes at Sea
# ---------------------------------------------------------------------------

def detect_anomaly_c_draft_change(
    vessel_timelines: dict[str, list[dict]],
    min_gap_hours: float = 2.0,
    min_draft_change_ratio: float = 0.05,
    min_distance_nm: float = 0.5,
    debug: bool = False,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int] | None]:
    """
    Detect draught changes > 5% during AIS blackouts > 2 hours.
    """
    anomalies_by_vessel: dict[str, list[dict[str, Any]]] = {}

    stats = {
        "pairs_total": 0,
        "pairs_gap_ok": 0,
        "pairs_draught_present": 0,
        "pairs_draught_change_ok": 0,
        "pairs_position_present": 0,
        "pairs_distance_ok": 0,
        "events_flagged": 0,
    }

    for mmsi, obs_list in vessel_timelines.items():
        if len(obs_list) < 2:
            continue

        vessel_anomalies: list[dict[str, Any]] = []

        for prev_obs, curr_obs in zip(obs_list, obs_list[1:]):
            stats["pairs_total"] += 1

            ts1 = prev_obs["ts"]
            ts2 = curr_obs["ts"]

            if not isinstance(ts1, datetime) or not isinstance(ts2, datetime):
                continue

            gap_hours = (ts2 - ts1).total_seconds() / 3600.0
            if gap_hours <= min_gap_hours:
                continue
            stats["pairs_gap_ok"] += 1

            draft1 = prev_obs["draught"]
            draft2 = curr_obs["draught"]

            if is_missing_float(draft1) or is_missing_float(draft2):
                continue
            if draft1 <= 0 or draft2 <= 0:
                continue
            stats["pairs_draught_present"] += 1

            draft_change_abs = abs(draft2 - draft1)
            draft_change_ratio = draft_change_abs / draft1

            if draft_change_ratio <= min_draft_change_ratio:
                continue
            stats["pairs_draught_change_ok"] += 1

            lat1 = prev_obs["lat"]
            lon1 = prev_obs["lon"]
            lat2 = curr_obs["lat"]
            lon2 = curr_obs["lon"]

            if (
                is_missing_float(lat1) or is_missing_float(lon1)
                or is_missing_float(lat2) or is_missing_float(lon2)
            ):
                continue
            stats["pairs_position_present"] += 1

            distance_nm = haversine_nm(lat1, lon1, lat2, lon2)
            if distance_nm < min_distance_nm:
                continue
            stats["pairs_distance_ok"] += 1

            vessel_anomalies.append(
                {
                    "type": "C",
                    "gap_start_ts": ts1,
                    "gap_end_ts": ts2,
                    "gap_hours": round(gap_hours, 3),
                    "start_draught": round(draft1, 3),
                    "end_draught": round(draft2, 3),
                    "draft_change_abs": round(draft_change_abs, 3),
                    "draft_change_pct": round(draft_change_ratio * 100.0, 3),
                    "start_lat": lat1,
                    "start_lon": lon1,
                    "end_lat": lat2,
                    "end_lon": lon2,
                    "distance_nm": round(distance_nm, 3),
                    "start_name": prev_obs.get("name", ""),
                    "end_name": curr_obs.get("name", ""),
                }
            )
            stats["events_flagged"] += 1

        if vessel_anomalies:
            anomalies_by_vessel[mmsi] = vessel_anomalies

    return anomalies_by_vessel, (stats if debug else None)


def summarize_anomaly_c(anomalies_by_vessel: dict[str, list[dict[str, Any]]]) -> list[tuple]:
    summary = []
    for mmsi, events in anomalies_by_vessel.items():
        count = len(events)
        max_pct = max(e["draft_change_pct"] for e in events)
        max_gap = max(e["gap_hours"] for e in events)
        summary.append((count, max_pct, max_gap, mmsi))
    summary.sort(reverse=True)
    return summary


# ---------------------------------------------------------------------------
# Anomaly B: Loitering & Transfers
# ---------------------------------------------------------------------------

def floor_time_to_bucket(ts: datetime, bucket_minutes: int) -> datetime:
    minute = (ts.minute // bucket_minutes) * bucket_minutes
    return ts.replace(minute=minute, second=0, microsecond=0)

def detect_anomaly_b_loitering_transfer(
    vessel_timelines: dict[str, list[dict]],
    max_distance_m: float = 500.0,
    max_sog_knots: float = 1.0,
    min_duration_hours: float = 2.0,
    bucket_minutes: int = 10,
    exclude_nav_statuses: set[str] | None = None,
    max_avg_distance_m: float = 250.0,
    max_event_distance_m: float = 350.0,
    allow_one_missing_bucket: bool = True,
) -> list[dict[str, Any]]:
    """
    Improved Anomaly B detector: loitering / potential ship-to-ship transfer.

    Assignment rule kept:
      - distance <= 500 m
      - SOG < 1 knot
      - duration > 2 hours

    Added precision improvements:
      1. Exclude obvious anchored / moored vessels
      2. Use average vessel position per time bucket instead of first point only
      3. Add stricter event-level distance filters
      4. Allow one missing bucket so AIS noise does not break a true event
    """

    # CHANGED:
    # If user does not pass statuses manually, exclude the two most common
    # false-positive harbor cases.
    if exclude_nav_statuses is None:
        exclude_nav_statuses = {"Moored", "At anchor"}

    # =========================================================
    # STEP 1: collect ALL low-speed observations per vessel/bucket
    # =========================================================
    # CHANGED:
    # Old version kept only the FIRST point per bucket.
    # New version accumulates all points, then averages them.
    # This reduces noise from one random AIS message.
    buckets: dict[datetime, dict[str, dict[str, Any]]] = defaultdict(dict)

    for mmsi, obs_list in vessel_timelines.items():
        for obs in obs_list:
            ts = obs.get("ts")
            lat = obs.get("lat")
            lon = obs.get("lon")
            sog = obs.get("sog")
            nav = (obs.get("nav") or "").strip()
            name = (obs.get("name") or "").strip()

            if not isinstance(ts, datetime):
                continue

            if is_missing_float(lat) or is_missing_float(lon):
                continue

            # Assignment rule kept: only low-speed observations
            if is_missing_float(sog) or sog >= max_sog_knots:
                continue

            # CHANGED:
            # Exclude obvious non-suspicious harbor statuses.
            # This is the biggest false-positive reduction.
            if nav in exclude_nav_statuses:
                continue

            bucket_ts = floor_time_to_bucket(ts, bucket_minutes)

            if mmsi not in buckets[bucket_ts]:
                buckets[bucket_ts][mmsi] = {
                    "lat_sum": 0.0,
                    "lon_sum": 0.0,
                    "sog_sum": 0.0,
                    "count": 0,
                    "name": name,
                    "nav": nav,
                }

            buckets[bucket_ts][mmsi]["lat_sum"] += lat
            buckets[bucket_ts][mmsi]["lon_sum"] += lon
            buckets[bucket_ts][mmsi]["sog_sum"] += sog
            buckets[bucket_ts][mmsi]["count"] += 1

            # Keep non-empty name if available
            if name and not buckets[bucket_ts][mmsi]["name"]:
                buckets[bucket_ts][mmsi]["name"] = name

    # =========================================================
    # STEP 2: average each vessel's position inside each bucket
    # =========================================================
    # CHANGED:
    # Instead of first raw point, represent vessel by mean position in bucket.
    averaged_buckets: dict[datetime, dict[str, dict[str, Any]]] = defaultdict(dict)

    for bucket_ts, vessel_map in buckets.items():
        for mmsi, acc in vessel_map.items():
            count = acc["count"]
            if count <= 0:
                continue

            averaged_buckets[bucket_ts][mmsi] = {
                "lat": acc["lat_sum"] / count,
                "lon": acc["lon_sum"] / count,
                "avg_sog": acc["sog_sum"] / count,
                "name": acc["name"],
                "nav": acc["nav"],
                "n_obs": count,
            }

    # =========================================================
    # STEP 3: compare vessel pairs inside each bucket
    # =========================================================
    pair_hits: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for bucket_ts, vessel_points in averaged_buckets.items():
        mmsis = sorted(vessel_points.keys())

        for m1, m2 in combinations(mmsis, 2):
            p1 = vessel_points[m1]
            p2 = vessel_points[m2]

            distance_nm = haversine_nm(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
            distance_m = distance_nm * 1852.0

            # Assignment rule kept:
            # pair must still satisfy the official <= 500m threshold
            if distance_m <= max_distance_m:
                pair_hits[(m1, m2)].append(
                    {
                        "bucket_ts": bucket_ts,
                        "distance_m": distance_m,
                        "name_1": p1.get("name", ""),
                        "name_2": p2.get("name", ""),
                    }
                )

    # =========================================================
    # STEP 4: merge consecutive buckets into long-duration events
    # =========================================================
    events: list[dict[str, Any]] = []

    bucket_delta_seconds = bucket_minutes * 60

    # CHANGED:
    # Old version required PERFECT continuity.
    # New version can tolerate one missing bucket.
    max_allowed_gap = bucket_delta_seconds * (2 if allow_one_missing_bucket else 1)

    min_buckets_required = int((min_duration_hours * 60) / bucket_minutes)

    for (m1, m2), hits in pair_hits.items():
        if not hits:
            continue

        hits.sort(key=lambda x: x["bucket_ts"])
        run = [hits[0]]

        for prev_hit, curr_hit in zip(hits, hits[1:]):
            dt = (curr_hit["bucket_ts"] - prev_hit["bucket_ts"]).total_seconds()

            if dt <= max_allowed_gap:
                run.append(curr_hit)
            else:
                if len(run) >= min_buckets_required:
                    distances = [x["distance_m"] for x in run]
                    avg_distance_m = sum(distances) / len(distances)
                    max_distance_seen = max(distances)

                    # CHANGED:
                    # Added event-level filters to reduce false positives.
                    # Pair may be <=500m in each bucket, but if it always sits
                    # near the edge (e.g. 490-500m), this is weaker evidence.
                    if avg_distance_m <= max_avg_distance_m and max_distance_seen <= max_event_distance_m:
                        duration_hours = (
                            (run[-1]["bucket_ts"] - run[0]["bucket_ts"]).total_seconds() / 3600.0
                        ) + (bucket_minutes / 60.0)

                        events.append(
                            {
                                "type": "B",
                                "mmsi_1": m1,
                                "mmsi_2": m2,
                                "start_ts": run[0]["bucket_ts"],
                                "end_ts": run[-1]["bucket_ts"],
                                "duration_hours": round(duration_hours, 3),
                                "n_buckets": len(run),
                                "max_distance_m": round(max_distance_seen, 2),
                                "avg_distance_m": round(avg_distance_m, 2),
                                "name_1": run[0].get("name_1", ""),
                                "name_2": run[0].get("name_2", ""),
                            }
                        )
                run = [curr_hit]

        # finalize last run
        if len(run) >= min_buckets_required:
            distances = [x["distance_m"] for x in run]
            avg_distance_m = sum(distances) / len(distances)
            max_distance_seen = max(distances)

            if avg_distance_m <= max_avg_distance_m and max_distance_seen <= max_event_distance_m:
                duration_hours = (
                    (run[-1]["bucket_ts"] - run[0]["bucket_ts"]).total_seconds() / 3600.0
                ) + (bucket_minutes / 60.0)

                events.append(
                    {
                        "type": "B",
                        "mmsi_1": m1,
                        "mmsi_2": m2,
                        "start_ts": run[0]["bucket_ts"],
                        "end_ts": run[-1]["bucket_ts"],
                        "duration_hours": round(duration_hours, 3),
                        "n_buckets": len(run),
                        "max_distance_m": round(max_distance_seen, 2),
                        "avg_distance_m": round(avg_distance_m, 2),
                        "name_1": run[0].get("name_1", ""),
                        "name_2": run[0].get("name_2", ""),
                    }
                )

    # Sort strongest events first
    events.sort(
        key=lambda e: (e["duration_hours"], -e["avg_distance_m"]),
        reverse=True,
    )
    return events


def summarize_anomaly_b(events: list[dict[str, Any]]) -> list[tuple]:
    summary = []
    for e in events:
        summary.append((e["duration_hours"], e["avg_distance_m"], e["mmsi_1"], e["mmsi_2"]))
    summary.sort(reverse=True)
    return summary


# ---------------------------------------------------------------------------
# DFSI calculation (Task 3)
# ---------------------------------------------------------------------------

def calculate_dfsi(
    anomaly_a: dict[str, list[dict[str, Any]]],
    anomaly_c: dict[str, list[dict[str, Any]]],
    anomaly_d: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """
    DFSI formula from the assignment image:

        DFSI = (Max Gap in Hours / 2)
             + (Total Impossible Distance Jump (Nautical Miles) / 10)
             + (C * 15)

    Where C = number of illicit Draft Changes detected for that vessel.

    Notes:
    - Max Gap in Hours comes from anomaly A events for that vessel.
    - Total Impossible Distance Jump is the sum of anomaly D distance_nm values.
    - Vessels are included if they were flagged by A, C, or D.
    """
    all_mmsi = set(anomaly_a) | set(anomaly_c) | set(anomaly_d)
    results: dict[str, dict[str, Any]] = {}

    for mmsi in all_mmsi:
        a_events = anomaly_a.get(mmsi, [])
        c_events = anomaly_c.get(mmsi, [])
        d_events = anomaly_d.get(mmsi, [])

        max_gap_hours = max((e["gap_hours"] for e in a_events), default=0.0)
        total_impossible_distance_nm = round(sum(e["distance_nm"] for e in d_events), 3)
        c_count = len(c_events)

        dfsi = round(
            (max_gap_hours / 2.0)
            + (total_impossible_distance_nm / 10.0)
            + (c_count * 15.0),
            3,
        )

        name = ""
        for events in (a_events, c_events, d_events):
            if events:
                name = events[0].get("start_name") or events[0].get("end_name") or ""
                if name:
                    break

        results[mmsi] = {
            "mmsi": mmsi,
            "name": name,
            "dfsi": dfsi,
            "max_gap_hours": round(max_gap_hours, 3),
            "total_impossible_distance_nm": total_impossible_distance_nm,
            "draft_change_count": c_count,
            "anomaly_a_count": len(a_events),
            "anomaly_c_count": c_count,
            "anomaly_d_count": len(d_events),
        }

    return results


def summarize_dfsi(dfsi_by_vessel: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        dfsi_by_vessel.values(),
        key=lambda x: (
            x["dfsi"],
            x["anomaly_d_count"],
            x["anomaly_c_count"],
            x["anomaly_a_count"],
            x["total_impossible_distance_nm"],
            x["max_gap_hours"],
            x["mmsi"],
        ),
        reverse=True,
    )
    return ranked
