import json
import sys
import os


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    if len(sys.argv) < 3:
        print("Usage: python vessel_lookup.py <results_dir> <MMSI>")
        sys.exit(1)

    output_dir = sys.argv[1]
    mmsi = sys.argv[2].strip()

    dfsi_path = os.path.join(output_dir, "dfsi_ranked.json")
    ranked = load_json(dfsi_path)
    vessel = next((v for v in ranked if v["mmsi"] == mmsi), None)

    if vessel:
        rank = next(i for i, v in enumerate(ranked, 1) if v["mmsi"] == mmsi)
        print(f"═══ DFSI Summary (rank #{rank} of {len(ranked)}) ═══")
        for k, v in vessel.items():
            print(f"  {k:35s}: {v}")
    else:
        print(f"MMSI {mmsi} not found in DFSI ranking (no anomalies flagged).")

    print()

    # ── Anomaly A ──────────────────────────────────────────────────────
    a_path = os.path.join(output_dir, "anomaly_a.json")
    anomaly_a = load_json(a_path)
    a_events = anomaly_a.get(mmsi, [])
    print(f"═══ Anomaly A — Going Dark ({len(a_events)} events) ═══")
    for i, e in enumerate(a_events, 1):
        gmaps_start = f"https://www.google.com/maps?q={e['start_lat']},{e['start_lon']}"
        gmaps_end = f"https://www.google.com/maps?q={e['end_lat']},{e['end_lon']}"
        directions = (
            f"https://www.google.com/maps/dir/"
            f"{e['start_lat']},{e['start_lon']}/"
            f"{e['end_lat']},{e['end_lon']}/"
        )
        print(f"\n  Event {i}:")
        print(f"    Disappeared : {e['gap_start_ts']}")
        print(f"    Reappeared  : {e['gap_end_ts']}")
        print(f"    Gap         : {e['gap_hours']} hours")
        print(f"    Distance    : {e['distance_nm']} nm")
        print(f"    Implied speed: {e['implied_speed_knots']} kn")
        print(f"    Start coord : {e['start_lat']:.5f}, {e['start_lon']:.5f}")
        print(f"    End coord   : {e['end_lat']:.5f}, {e['end_lon']:.5f}")
        print(f"    Name (start): {e.get('start_name', '')}")
        print(f"    Name (end)  : {e.get('end_name', '')}")
        print(f"    Map (vanish)   : {gmaps_start}")
        print(f"    Map (reappear) : {gmaps_end}")
        print(f"    Map (both)     : {directions}")

    # ── Anomaly B ──────────────────────────────────────────────────────
    b_path = os.path.join(output_dir, "anomaly_b.json")
    anomaly_b = load_json(b_path)
    b_events = [e for e in anomaly_b if e["mmsi_1"] == mmsi or e["mmsi_2"] == mmsi]
    print(f"\n═══ Anomaly B — Loitering/Transfer ({len(b_events)} events) ═══")
    for i, e in enumerate(b_events, 1):
        other = e["mmsi_2"] if e["mmsi_1"] == mmsi else e["mmsi_1"]
        print(f"\n  Event {i}:")
        print(f"    Paired with : MMSI {other}")
        print(f"    Start       : {e['start_ts']}")
        print(f"    End         : {e['end_ts']}")
        print(f"    Duration    : {e['duration_hours']} hours")
        print(f"    Avg distance: {e['avg_distance_m']} m")
        print(f"    Max distance: {e['max_distance_m']} m")
        print(f"    Name 1      : {e.get('name_1', '')}")
        print(f"    Name 2      : {e.get('name_2', '')}")

    # ── Anomaly C ──────────────────────────────────────────────────────
    c_path = os.path.join(output_dir, "anomaly_c.json")
    anomaly_c = load_json(c_path)
    c_events = anomaly_c.get(mmsi, [])
    print(f"\n═══ Anomaly C — Draft Change ({len(c_events)} events) ═══")
    for i, e in enumerate(c_events, 1):
        print(f"\n  Event {i}:")
        print(f"    Gap         : {e['gap_start_ts']} → {e['gap_end_ts']}")
        print(f"    Gap hours   : {e['gap_hours']}")
        print(f"    Draught     : {e['start_draught']} → {e['end_draught']}")
        print(f"    Change      : {e['draft_change_pct']}%")
        print(f"    Distance    : {e['distance_nm']} nm")

    # ── Anomaly D ──────────────────────────────────────────────────────
    d_path = os.path.join(output_dir, "anomaly_d.json")
    anomaly_d = load_json(d_path)
    d_events = anomaly_d.get(mmsi, [])
    print(f"\n═══ Anomaly D — Teleportation ({len(d_events)} events) ═══")
    for i, e in enumerate(d_events, 1):
        print(f"\n  Event {i}:")
        print(f"    Time        : {e['start_ts']} → {e['end_ts']}")
        print(f"    Speed       : {e['implied_speed_knots']} kn")
        print(f"    Distance    : {e['distance_nm']} nm")
        print(f"    Start coord : {e['start_lat']:.5f}, {e['start_lon']:.5f}")
        print(f"    End coord   : {e['end_lat']:.5f}, {e['end_lon']:.5f}")

    if not (a_events or b_events or c_events or d_events):
        print("\nNo anomaly events found for this MMSI.")


if __name__ == "__main__":
    main()