#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Original Danish AIS column name -> internal name.
# Backtick quoting handles names that contain spaces and special characters.
_COLUMNS = {
    "# Timestamp": "ts_raw",
    "Type of mobile": "mobile_type",
    "MMSI": "mmsi",
    "Latitude": "lat",
    "Longitude": "lon",
    "Navigational status": "nav_status",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "Name": "name",
    "Ship type": "ship_type",
}

# Danish AIS timestamps: "13/12/2021 03:27:05"
_TS_FORMAT = "dd/MM/yyyy HH:mm:ss"


def load_ais(spark: SparkSession, path_glob: str) -> DataFrame:
    """Load one or many AIS CSV files into a typed DataFrame.

    path_glob may be a single file or a glob such as data/*.csv.
    PERMISSIVE mode turns malformed rows into nulls rather than crashing;
    the validity filter in clean.py removes them downstream.
    """
    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(path_glob)

    selected = raw.select(
        [F.col(f"`{src}`").alias(dst) for src, dst in _COLUMNS.items()]
    )

    return (
        selected.withColumn("ts", F.to_timestamp("ts_raw", _TS_FORMAT))
        .withColumn("mmsi", F.col("mmsi").cast("long"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .withColumn("cog", F.col("cog").cast("double"))
        .drop("ts_raw")
    )
