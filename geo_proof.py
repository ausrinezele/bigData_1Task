import json
import sys
import os


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "results"

    dfsi_path = os.path.join(output_dir, "dfsi_ranked.json")
    ranked = load_json(dfsi_path)

    if not ranked:
        print("No flagged vessels found.")
        return

    top = ranked[0]
    mmsi = top["mmsi"]
    print(f"═══ Top DFSI Vessel ═══")
    print(f"  MMSI  : {mmsi}")
    print(f"  Name  : {top.get('name', 'unknown')}")
    print(f"  DFSI  : {top['dfsi']}")
    print(f"  A events (Going Dark)     : {top['anomaly_a_count']}")
    print(f"  C events (Draft Change)   : {top['anomaly_c_count']}")
    print(f"  D events (Teleportation)  : {top['anomaly_d_count']}")
    print(f"  Max gap (hours)           : {top['max_gap_hours']}")
    print(f"  Total impossible dist (nm): {top['total_impossible_distance_nm']}")
    print()

    coords = []

    # Anomaly A
    a_path = os.path.join(output_dir, "anomaly_a.json")
    anomaly_a = load_json(a_path)
    if mmsi in anomaly_a:
        for e in anomaly_a[mmsi]:
            coords.append({
                "type": "A (went dark)",
                "lat": e["start_lat"], "lon": e["start_lon"],
                "ts": e["gap_start_ts"], "label": "disappearance"
            })
            coords.append({
                "type": "A (reappeared)",
                "lat": e["end_lat"], "lon": e["end_lon"],
                "ts": e["gap_end_ts"], "label": "reappearance"
            })

    # Anomaly C
    c_path = os.path.join(output_dir, "anomaly_c.json")
    anomaly_c = load_json(c_path)
    if mmsi in anomaly_c:
        for e in anomaly_c[mmsi]:
            coords.append({
                "type": f"C (draft {e['start_draught']}→{e['end_draught']})",
                "lat": e["start_lat"], "lon": e["start_lon"],
                "ts": e["gap_start_ts"], "label": "draft change start"
            })

    # Anomaly D
    d_path = os.path.join(output_dir, "anomaly_d.json")
    anomaly_d = load_json(d_path)
    if mmsi in anomaly_d:
        for e in anomaly_d[mmsi]:
            coords.append({
                "type": f"D ({e['implied_speed_knots']} kn)",
                "lat": e["start_lat"], "lon": e["start_lon"],
                "ts": e["start_ts"], "label": "ping 1"
            })
            coords.append({
                "type": f"D ({e['implied_speed_knots']} kn)",
                "lat": e["end_lat"], "lon": e["end_lon"],
                "ts": e["end_ts"], "label": "ping 2"
            })

    if not coords:
        print("No coordinates found for this vessel.")
        return

    print(f"═══ Coordinates ({len(coords)} points) ═══\n")
    for c in coords:
        gmaps = f"https://www.google.com/maps?q={c['lat']},{c['lon']}"
        print(f"  [{c['type']}] {c['label']}")
        print(f"    Time : {c['ts']}")
        print(f"    Coord: {c['lat']:.5f}, {c['lon']:.5f}")
        print(f"    Map  : {gmaps}")
        print()

    a_coords = [c for c in coords if c["type"].startswith("A")]
    if a_coords:
        print("═══ Most suspicious Going Dark event ═══\n")
        vanish = a_coords[0]
        reappear = a_coords[1] if len(a_coords) > 1 else None
        print(f"  Vanished  : {vanish['lat']:.5f}, {vanish['lon']:.5f} at {vanish['ts']}")
        if reappear:
            print(f"  Reappeared: {reappear['lat']:.5f}, {reappear['lon']:.5f} at {reappear['ts']}")

            directions = (
                f"https://www.google.com/maps/dir/"
                f"{vanish['lat']},{vanish['lon']}/"
                f"{reappear['lat']},{reappear['lon']}/"
            )
            print(f"\n  Screenshot this link (shows both points):")
            print(f"  {directions}")


if __name__ == "__main__":
    main()