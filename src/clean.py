#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

# Cleaning and pre-processing stage.
#
# Filters are applied cheapest-first so every later, more expensive step
# sees as little data as possible:
#
#   1. Basic validity  — drop nulls, sentinel coords, non-vessel reports
#   2. Time window     — restrict to December 2021
#   3. Geo filter      — bounding box pre-filter, then exact Haversine <= 50 nm
#   4. GPS jump removal — per-vessel implied-speed test (window function)
#   5. Service vessels — drop rescue / coast-guard / pilot vessels
#   6. Moving only     — SOG >= MIN_SOG_KNOTS (removes anchored / docked)

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config
from .spark_utils import haversine_km


def basic_validity(df: DataFrame) -> DataFrame:
    # Keep only rows that can possibly be a usable moving-vessel position fix.
    # AIS uses 91.0 / 181.0 as "not available" sentinels; (0, 0) is the
    # "null island" artefact from uninitialised transponders.
    return df.filter(
        F.col("ts").isNotNull()
        & F.col("mmsi").isNotNull()
        & (F.col("mmsi") > 0)
        & F.col("lat").isNotNull()
        & F.col("lon").isNotNull()
        & F.col("lat").between(config.LAT_MIN, config.LAT_MAX)
        & F.col("lon").between(config.LON_MIN, config.LON_MAX)
        & (F.col("lat") != 91.0)
        & (F.col("lon") != 181.0)
        & ~((F.col("lat") == 0.0) & (F.col("lon") == 0.0))
        & (
            F.col("mobile_type").isNull()
            | F.col("mobile_type").isin("Class A", "Class B")
        )
    )


def filter_time(df: DataFrame) -> DataFrame:
    # Restrict to the assignment window, inclusive of both end-of-day bounds.
    start = F.to_timestamp(F.lit(config.START_DATE + " 00:00:00"))
    end = F.to_timestamp(F.lit(config.END_DATE + " 23:59:59"))
    return df.filter((F.col("ts") >= start) & (F.col("ts") <= end))


def filter_area(df: DataFrame) -> DataFrame:
    # Two-stage geo filter: cheap bounding box first, then exact Haversine.
    # The box discards the vast majority of rows with a simple column
    # comparison so the trigonometry only runs on a small fraction of data.
    dlat = config.RADIUS_KM / 111.32
    dlon = config.RADIUS_KM / (111.32 * math.cos(math.radians(config.CENTER_LAT)))

    boxed = df.filter(
        F.col("lat").between(config.CENTER_LAT - dlat, config.CENTER_LAT + dlat)
        & F.col("lon").between(config.CENTER_LON - dlon, config.CENTER_LON + dlon)
    )

    dist = haversine_km(
        F.col("lat"),
        F.col("lon"),
        F.lit(config.CENTER_LAT),
        F.lit(config.CENTER_LON),
    )
    return boxed.withColumn("dist_center_km", dist).filter(
        F.col("dist_center_km") <= config.RADIUS_KM
    )


def remove_gps_jumps(df: DataFrame) -> DataFrame:
    # For each MMSI ordered by time, compute the implied speed to the previous
    # fix. Fixes implying more than MAX_PLAUSIBLE_SPEED_KNOTS are GPS noise
    # and discarded before the proximity search so a teleporting point can
    # never be mistaken for a collision.
    w = Window.partitionBy("mmsi").orderBy("ts")

    enriched = (
        df.withColumn("_prev_lat", F.lag("lat").over(w))
        .withColumn("_prev_lon", F.lag("lon").over(w))
        .withColumn("_prev_ts", F.lag("ts").over(w))
    )

    step_km = haversine_km(
        F.col("_prev_lat"),
        F.col("_prev_lon"),
        F.col("lat"),
        F.col("lon"),
    )
    dt_sec = F.col("ts").cast("long") - F.col("_prev_ts").cast("long")

    enriched = (
        enriched.withColumn("_step_km", step_km)
        .withColumn("_dt_sec", dt_sec)
        .withColumn(
            "_implied_knots",
            F.when(
                F.col("_dt_sec") > 0,
                (F.col("_step_km") / config.NM_TO_KM) / (F.col("_dt_sec") / 3600.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    return enriched.filter(
        F.col("_prev_ts").isNull()
        | (F.col("_implied_knots") <= config.MAX_PLAUSIBLE_SPEED_KNOTS)
    ).drop(
        "_prev_lat", "_prev_lon", "_prev_ts", "_step_km", "_dt_sec", "_implied_knots"
    )


def drop_service_vessels(df: DataFrame) -> DataFrame:
    # Remove rescue / coast-guard / pilot vessels that cluster around an
    # accident scene and would otherwise appear as false collision candidates
    # near the impact coordinates.
    name = F.upper(F.coalesce(F.col("name"), F.lit("")))
    return df.filter(
        ~name.contains("RESCUE")
        & ~name.contains("KBV")
        & ~name.contains("SVITZER")
        & ~name.contains("PILOT")
        & ~name.contains("SAR")
    )


def moving_only(df: DataFrame) -> DataFrame:
    # Keep only fixes where the vessel is under way. Stationary vessels
    # report SOG ~ 0 and are dropped so they can never form a false
    # zero-distance pair with another anchored vessel nearby.
    return df.filter(
        F.col("sog").isNotNull()
        & (F.col("sog") >= config.MIN_SOG_KNOTS)
        & (F.col("sog") <= config.MAX_SOG_KNOTS)
    )


def resolve_names(df: DataFrame) -> DataFrame:
    # One row per MMSI with the best-known vessel name — taken as the first
    # non-null name in chronological order.
    named = df.filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
    w = Window.partitionBy("mmsi").orderBy(F.col("ts").asc())
    return (
        named.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("mmsi", F.trim(F.col("name")).alias("vessel_name"))
    )
