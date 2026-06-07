#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

# Collision detection stage.
#
# The naive approach — an all-pairs Cartesian self-join — is O(N²) and
# infeasible on a month of AIS data. Instead every fix is assigned a
# (time_bin, cell_lat, cell_lon) bucket key and only fixes sharing a bucket
# are compared. Two vessels that physically touch must occupy the same small
# region at the same instant, so they are guaranteed to share a bucket.
#
# To avoid missing pairs that straddle a cell or bin boundary, the left side
# of the join is expanded into its 3×3×3 = 27 neighbouring buckets; the right
# side stays in its single home bucket. Any pair within one cell / one bin of
# each other will therefore meet.
#
# Each candidate pair is scored on multiple signals simultaneously. Pure
# proximity is not sufficient — two vessels in a tug escort or refuelling
# operation can be metres apart without any collision. The danger score
# requires proximity AND behavioural change (SOG / COG delta) to rank a real
# collision above routine close-quarters operations.

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config
from .spark_utils import haversine_km


def _add_history(df: DataFrame) -> DataFrame:
    # For each vessel ordered by time, compute SOG/COG deltas relative to the
    # previous ping and flag the last known signal. These are the behavioural
    # signals used in danger scoring.
    w = Window.partitionBy("mmsi").orderBy("ts")

    df = (
        df.withColumn("_prev_sog", F.lag("sog").over(w))
        .withColumn("_prev_cog", F.lag("cog").over(w))
        .withColumn("_prev_ts", F.lag("ts").over(w))
        .withColumn("_next_ts", F.lead("ts").over(w))
    )

    # SOG delta: negative = deceleration, positive = acceleration
    df = df.withColumn(
        "delta_sog",
        F.round(F.col("sog") - F.col("_prev_sog"), 2),
    )

    # COG delta: handle 360° wraparound (e.g. 350° -> 10° = 20°, not 340°)
    raw_diff = F.abs(F.col("cog") - F.col("_prev_cog"))
    df = df.withColumn(
        "delta_cog",
        F.round(
            F.when(raw_diff.isNull(), 0.0).otherwise(
                F.when(raw_diff > 180.0, 360.0 - raw_diff).otherwise(raw_diff)
            ),
            2,
        ),
    )

    # Time gap to previous ping — used to penalise sparse reporters
    df = df.withColumn(
        "report_interval_sec",
        F.unix_timestamp("ts") - F.unix_timestamp("_prev_ts"),
    )

    # Last-signal flag: no subsequent ping means the vessel went silent —
    # the strongest indicator of capsize or sinking
    df = df.withColumn(
        "is_last_signal",
        F.when(F.col("_next_ts").isNull(), 1).otherwise(0),
    )

    return df.drop("_prev_sog", "_prev_cog", "_prev_ts", "_next_ts")


def _bucketize(df: DataFrame) -> DataFrame:
    # Assign each fix to an integer time bin and spatial grid cell.
    return (
        df.withColumn(
            "time_bin",
            (F.col("ts").cast("long") / config.TIME_BIN_SECONDS).cast("long"),
        )
        .withColumn(
            "cell_lat",
            F.floor((F.col("lat") - config.CENTER_LAT) / config.GRID_CELL_DEG).cast(
                "int"
            ),
        )
        .withColumn(
            "cell_lon",
            F.floor((F.col("lon") - config.CENTER_LON) / config.GRID_CELL_DEG).cast(
                "int"
            ),
        )
    )


def _find_candidates(df: DataFrame) -> DataFrame:
    # Bucketed self-join: left side expanded into 27 neighbour buckets,
    # right side in its single home bucket.
    bucketed = _bucketize(df).select(
        "mmsi",
        "ts",
        "lat",
        "lon",
        "sog",
        "cog",
        "delta_sog",
        "delta_cog",
        "report_interval_sec",
        "is_last_signal",
        "time_bin",
        "cell_lat",
        "cell_lon",
    )

    offsets = F.array(F.lit(-1), F.lit(0), F.lit(1))

    left = (
        bucketed.withColumn("_dbin", F.explode(offsets))
        .withColumn("_dlat", F.explode(offsets))
        .withColumn("_dlon", F.explode(offsets))
        .withColumn("k_bin", F.col("time_bin") + F.col("_dbin"))
        .withColumn("k_lat", F.col("cell_lat") + F.col("_dlat"))
        .withColumn("k_lon", F.col("cell_lon") + F.col("_dlon"))
        .select(
            F.col("mmsi").alias("mmsi_a"),
            F.col("ts").alias("ts_a"),
            F.col("lat").alias("lat_a"),
            F.col("lon").alias("lon_a"),
            F.col("delta_sog").alias("delta_sog_a"),
            F.col("delta_cog").alias("delta_cog_a"),
            F.col("report_interval_sec").alias("report_interval_a"),
            F.col("is_last_signal").alias("is_last_a"),
            "k_bin",
            "k_lat",
            "k_lon",
        )
    )

    right = bucketed.select(
        F.col("mmsi").alias("mmsi_b"),
        F.col("ts").alias("ts_b"),
        F.col("lat").alias("lat_b"),
        F.col("lon").alias("lon_b"),
        F.col("delta_sog").alias("delta_sog_b"),
        F.col("delta_cog").alias("delta_cog_b"),
        F.col("report_interval_sec").alias("report_interval_b"),
        F.col("is_last_signal").alias("is_last_b"),
        F.col("time_bin").alias("k_bin"),
        F.col("cell_lat").alias("k_lat"),
        F.col("cell_lon").alias("k_lon"),
    )

    joined = (
        left.join(right, on=["k_bin", "k_lat", "k_lon"], how="inner")
        .filter(F.col("mmsi_a") < F.col("mmsi_b"))
        .withColumn(
            "dt_sec",
            F.abs(F.col("ts_a").cast("long") - F.col("ts_b").cast("long")),
        )
        .filter(F.col("dt_sec") <= config.MAX_TIME_DIFF_SECONDS)
    )

    with_dist = joined.withColumn(
        "dist_m",
        haversine_km(
            F.col("lat_a"),
            F.col("lon_a"),
            F.col("lat_b"),
            F.col("lon_b"),
        )
        * 1000.0,
    )

    return (
        with_dist.filter(F.col("dist_m") <= config.COLLISION_DISTANCE_M)
        .select(
            "mmsi_a",
            "mmsi_b",
            "ts_a",
            "ts_b",
            "lat_a",
            "lon_a",
            "lat_b",
            "lon_b",
            "dist_m",
            "dt_sec",
            "delta_sog_a",
            "delta_sog_b",
            "delta_cog_a",
            "delta_cog_b",
            "report_interval_a",
            "is_last_a",
            "is_last_b",
        )
        .dropDuplicates(["mmsi_a", "mmsi_b", "ts_a", "ts_b"])
    )


def _score(df: DataFrame) -> DataFrame:
    # Danger score: proximity + behavioural signals scored independently and
    # summed. A real collision requires proximity AND behavioural change, so
    # a tug escort (close but smooth SOG/COG) scores much lower than a vessel
    # that decelerates sharply and goes silent.

    dist_score = (
        F.when(F.col("dist_m") <= 20.0, config.SCORE_DIST_CLOSE)
        .when(F.col("dist_m") <= 50.0, config.SCORE_DIST_MEDIUM)
        .otherwise(config.SCORE_DIST_FAR)
    )

    # COG change on either vessel
    max_cog = F.greatest(
        F.coalesce(F.col("delta_cog_a"), F.lit(0.0)),
        F.coalesce(F.col("delta_cog_b"), F.lit(0.0)),
    )
    cog_score = (
        F.when(max_cog > 30.0, config.SCORE_COG_LARGE)
        .when(max_cog > 15.0, config.SCORE_COG_MEDIUM)
        .otherwise(F.lit(0))
    )

    # SOG change on either vessel
    min_sog_delta = F.least(
        F.coalesce(F.col("delta_sog_a"), F.lit(0.0)),
        F.coalesce(F.col("delta_sog_b"), F.lit(0.0)),
    )
    max_sog_delta = F.greatest(
        F.coalesce(F.col("delta_sog_a"), F.lit(0.0)),
        F.coalesce(F.col("delta_sog_b"), F.lit(0.0)),
    )
    sog_score = (
        F.when(min_sog_delta <= -2.0, config.SCORE_SOG_DECEL)
        .when(max_sog_delta >= 2.0, config.SCORE_SOG_ACCEL)
        .otherwise(F.lit(0))
    )

    # Sparse reporting penalty
    sparse_penalty = F.when(
        F.col("report_interval_a") > 120,
        config.SCORE_SPARSE_PENALTY,
    ).otherwise(F.lit(0))

    # Last-signal bonus — vessel went silent immediately after encounter
    last_signal_bonus = F.when(
        (F.col("is_last_a") == 1) | (F.col("is_last_b") == 1),
        config.SCORE_LAST_SIGNAL,
    ).otherwise(F.lit(0))

    return df.withColumn(
        "danger_score",
        dist_score + cog_score + sog_score + sparse_penalty + last_signal_bonus,
    )


def _best_per_pair(df: DataFrame) -> DataFrame:
    # For each vessel pair keep only the single closest-approach record.
    w = Window.partitionBy("mmsi_a", "mmsi_b").orderBy(F.col("dist_m").asc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def detect(moving: DataFrame):
    """Run full detection. Returns (best_row, ranked_DataFrame).

    The collision is the pair with the highest danger score. The collision
    timestamp and position are taken as the midpoint of the two vessels at
    the closest-approach instant.
    """
    candidates = _find_candidates(_add_history(moving))
    scored = _score(candidates)
    best_per = _best_per_pair(scored)

    ranked = (
        best_per.withColumn(
            "collision_lat",
            (F.col("lat_a") + F.col("lat_b")) / 2.0,
        )
        .withColumn(
            "collision_lon",
            (F.col("lon_a") + F.col("lon_b")) / 2.0,
        )
        .withColumn(
            "collision_ts",
            F.when(F.col("ts_a") <= F.col("ts_b"), F.col("ts_a")).otherwise(
                F.col("ts_b")
            ),
        )
        .orderBy(F.col("danger_score").desc(), F.col("dist_m").asc())
    )

    best = ranked.limit(1).collect()
    return (best[0] if best else None), ranked
