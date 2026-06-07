#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

import os

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from . import config


def build_spark(app_name: str = "ais-collision-detection") -> SparkSession:
    # Haversine and bucketed self-join are CPU-bound; AQE lets Spark coalesce
    # shuffle partitions automatically after the geo filter shrinks the data.
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", "/tmp/spark-local")
        .getOrCreate()
    )


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    # Implemented as native Spark column expressions — no Python UDF — so the
    # computation stays inside the JVM/Catalyst engine and is fully vectorised.
    r = config.EARTH_RADIUS_KM
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2.0), 2) + (
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2.0), 2)
    )
    return F.lit(2.0 * r) * F.asin(F.sqrt(a))
