#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

# Entry point — orchestrates the full pipeline:
#   ingest -> clean -> detect -> visualize
# and writes results.txt and the trajectory map to the output directory.
#
# Run inside the container:
#   python -m src.main
#
# Configurable via environment variables:
#   AIS_DATA_GLOB   path glob for CSV files  (default: data_arch/aisdk-2021-12/*.csv)
#   AIS_OUTPUT_DIR  output directory         (default: outputs)

import os
import sys

from pyspark.storagelevel import StorageLevel

from . import clean, config, detect, visualize
from .ingest import load_ais
from .spark_utils import build_spark


def main() -> int:
    data_glob = os.environ.get("AIS_DATA_GLOB", "data_arch/aisdk-2021-12/*.csv")
    out_dir = os.environ.get("AIS_OUTPUT_DIR", "outputs")
    os.makedirs(out_dir, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Ingest
    # ------------------------------------------------------------------
    print(f"[1/5] Loading AIS data from: {data_glob}")
    raw = load_ais(spark, data_glob)

    # ------------------------------------------------------------------
    # 2. Clean
    # ------------------------------------------------------------------
    print("[2/5] Cleaning: validity -> time -> area -> GPS-jump removal")
    df = clean.basic_validity(raw)
    df = clean.filter_time(df)
    df = clean.filter_area(df)
    df = clean.remove_gps_jumps(df)

    # Persist the cleaned in-area dataset — reused for name resolution
    # and trajectory extraction without recomputing the full pipeline.
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    n = df.count()
    print(f"      {n:,} fixes inside {config.RADIUS_NM:.0f} nm after cleaning")

    # ------------------------------------------------------------------
    # 3. Detect
    # ------------------------------------------------------------------
    print("[3/5] Filtering to moving vessels and detecting collision")
    moving = clean.moving_only(clean.drop_service_vessels(df))
    best, ranked = detect.detect(moving)

    if best is None:
        print(
            f"No vessel pair came within {config.COLLISION_DISTANCE_M:.0f} m. "
            "Nothing to report."
        )
        spark.stop()
        return 1

    # ------------------------------------------------------------------
    # 4. Results
    # ------------------------------------------------------------------
    print("[4/5] Writing results")
    names = clean.resolve_names(df)
    name_map = {r["mmsi"]: r["vessel_name"] for r in names.collect()}
    name_a = name_map.get(best["mmsi_a"], "UNKNOWN")
    name_b = name_map.get(best["mmsi_b"], "UNKNOWN")

    top_rows = ranked.limit(config.TOP_N_EVENTS).collect()

    lines = []
    lines.append("=" * 70)
    lines.append("COLLISION DETECTION RESULT")
    lines.append("=" * 70)
    lines.append(f"Vessel A  : {name_a}  (MMSI {best['mmsi_a']})")
    lines.append(f"Vessel B  : {name_b}  (MMSI {best['mmsi_b']})")
    lines.append(f"Time      : {best['collision_ts']} UTC")
    lines.append(
        f"Position  : lat {best['collision_lat']:.6f}, lon {best['collision_lon']:.6f}"
    )
    lines.append(f"Distance  : {best['dist_m']:.1f} m (closest approach)")
    lines.append(f"Danger score: {best['danger_score']}")
    lines.append("")
    lines.append(f"Top {config.TOP_N_EVENTS} events by danger score:")
    lines.append("-" * 70)
    for r in top_rows:
        a = name_map.get(r["mmsi_a"], "?")
        b = name_map.get(r["mmsi_b"], "?")
        lines.append(
            f"  score {r['danger_score']:>4}  dist {r['dist_m']:>7.1f} m"
            f"  {r['collision_ts']}"
            f"  {r['mmsi_a']} ({a}) <-> {r['mmsi_b']} ({b})"
        )

    report = "\n".join(lines)
    print("\n" + report + "\n")

    results_path = os.path.join(out_dir, "results.txt")
    with open(results_path, "w") as fh:
        fh.write(report + "\n")
    print(f"      Wrote {results_path}")

    # ------------------------------------------------------------------
    # 5. Visualize
    # ------------------------------------------------------------------
    print("[5/5] Generating trajectory map")
    track_pdf = visualize.extract_window(
        df, best["mmsi_a"], best["mmsi_b"], best["collision_ts"]
    )
    map_path = os.path.join(out_dir, "trajectory_map.html")
    visualize.plot_trajectory(
        track_pdf,
        best["mmsi_a"],
        name_a,
        best["mmsi_b"],
        name_b,
        best["collision_ts"],
        best["collision_lat"],
        best["collision_lon"],
        map_path,
    )
    print(f"      Wrote {map_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
