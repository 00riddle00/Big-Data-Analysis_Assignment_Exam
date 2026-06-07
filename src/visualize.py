#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

# Visualisation stage.
#
# Spark does the heavy lifting; by the time we plot we are dealing with at
# most a few hundred fixes (two vessels over a 20-minute window), so we
# collect that tiny slice to the driver and render it with Folium.
#
# Output: an interactive HTML trajectory map saved to the output directory.

from datetime import timedelta

import folium
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config


def extract_window(
    cleaned: DataFrame,
    mmsi_a: int,
    mmsi_b: int,
    collision_ts,
) -> "pd.DataFrame":
    """Return a small Pandas DataFrame of both vessels' fixes around impact."""
    lo = collision_ts - timedelta(minutes=config.WINDOW_MINUTES)
    hi = collision_ts + timedelta(minutes=config.WINDOW_MINUTES)

    return (
        cleaned.filter(F.col("mmsi").isin([mmsi_a, mmsi_b]))
        .filter((F.col("ts") >= F.lit(lo)) & (F.col("ts") <= F.lit(hi)))
        .select("mmsi", "ts", "lat", "lon", "sog", "cog")
        .orderBy("mmsi", "ts")
        .toPandas()
    )


def _sog_color(sog) -> str:
    # Color-code each ping by speed — instantly shows where vessels
    # decelerated (red/orange) approaching the collision point.
    try:
        s = float(sog)
    except (TypeError, ValueError):
        return "blue"
    if s <= 0.1:
        return "darkblue"
    if s <= 1.0:
        return "red"
    if s <= 5.0:
        return "orange"
    if s <= 12.0:
        return "yellow"
    return "green"


def plot_trajectory(
    track_pdf,
    mmsi_a: int,
    name_a: str,
    mmsi_b: int,
    name_b: str,
    collision_ts,
    collision_lat: float,
    collision_lon: float,
    out_path: str,
) -> str:
    """Render an interactive Folium map of both vessel trajectories."""

    m = folium.Map(
        location=[collision_lat, collision_lon],
        zoom_start=13,
        tiles="CartoDB.VoyagerLabelsUnder",
    )

    vessel_styles = {
        mmsi_a: {"color": "#555555", "label": f"{name_a} (MMSI {mmsi_a})"},
        mmsi_b: {"color": "#000000", "label": f"{name_b} (MMSI {mmsi_b})"},
    }

    for mmsi, style in vessel_styles.items():
        pts = track_pdf[track_pdf["mmsi"] == mmsi].sort_values("ts")
        if pts.empty:
            continue

        # Trajectory line
        coords = list(zip(pts["lat"], pts["lon"]))
        folium.PolyLine(
            coords,
            color=style["color"],
            weight=3,
            opacity=0.7,
            tooltip=style["label"],
        ).add_to(m)

        # Per-ping circle markers colored by SOG
        for _, row in pts.iterrows():
            popup = (
                f"<b>{style['label']}</b><br>"
                f"Time: {row['ts']}<br>"
                f"SOG: {row['sog']} kt<br>"
                f"COG: {row['cog']}°"
            )
            folium.CircleMarker(
                location=(row["lat"], row["lon"]),
                radius=5,
                color="black",
                weight=1,
                fill=True,
                fill_color=_sog_color(row["sog"]),
                fill_opacity=1.0,
                popup=folium.Popup(popup, max_width=250),
            ).add_to(m)

        # Start marker
        folium.Marker(
            location=(pts.iloc[0]["lat"], pts.iloc[0]["lon"]),
            icon=folium.Icon(color="blue", icon="play"),
            tooltip=f"Start: {style['label']}",
        ).add_to(m)

    # Collision marker
    folium.Marker(
        location=(collision_lat, collision_lon),
        icon=folium.Icon(color="red", icon="exclamation-sign"),
        popup=folium.Popup(
            "<b>COLLISION</b><br>"
            f"{collision_ts} UTC<br>"
            f"lat {collision_lat:.5f}, lon {collision_lon:.5f}",
            max_width=300,
        ),
        tooltip="Collision point",
    ).add_to(m)

    # Speed legend
    legend_html = """
    <div style="position:fixed;bottom:40px;left:40px;width:180px;
                border:2px solid grey;z-index:9999;font-size:13px;
                background:white;opacity:0.9;padding:10px;border-radius:6px;">
    <b>Speed (SOG):</b><br>
    <span style="color:green;">&#9679;</span> &gt; 12 kt<br>
    <span style="color:yellow;text-shadow:0 0 1px #999;">&#9679;</span> 5–12 kt<br>
    <span style="color:orange;">&#9679;</span> 1–5 kt<br>
    <span style="color:red;">&#9679;</span> 0.1–1 kt<br>
    <span style="color:darkblue;">&#9679;</span> &lt; 0.1 kt
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(out_path)
    return out_path
