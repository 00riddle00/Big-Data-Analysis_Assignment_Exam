#!/usr/bin/env python3
# vim: set ft=python tw=88 nu ai et ts=4 sw=4:

# Central configuration for the AIS collision-detection pipeline.
# All thresholds and parameters live here so behaviour is auditable in one
# place and easy to tune without touching the processing logic.

# ---------------------------------------------------------------------------
# Area of interest
# ---------------------------------------------------------------------------

# Centre point given by the assignment (Bornholmsgat, Baltic Sea).
CENTER_LAT = 55.225000
CENTER_LON = 14.245000

# Search radius. 1 nautical mile = 1.852 km.
RADIUS_NM = 50.0
NM_TO_KM = 1.852
RADIUS_KM = RADIUS_NM * NM_TO_KM  # ~92.6 km

# ---------------------------------------------------------------------------
# Time window (assignment: full December 2021)
# ---------------------------------------------------------------------------

START_DATE = "2021-12-01"  # inclusive
END_DATE = "2021-12-31"  # inclusive

# ---------------------------------------------------------------------------
# Vessel movement filter
# ---------------------------------------------------------------------------

# Speed Over Ground below which a vessel is treated as stationary
# (anchored / moored / docked). Removes the "two ships docked side by side"
# false-positive trap.
MIN_SOG_KNOTS = 1.0

# Upper bound — helicopters, coast-guard speedboats, and GPS anomalies
# all exceed this. Cargo vessels physically cannot.
MAX_SOG_KNOTS = 45.0

# Minimum number of pings for a vessel to be included in analysis.
# Vessels with fewer pings are likely noise or AIS relay artefacts.
MIN_PING_COUNT = 10

# ---------------------------------------------------------------------------
# GPS-noise / anomaly filter
# ---------------------------------------------------------------------------

# Implied speed between two consecutive fixes of the same vessel above which
# the fix is treated as a GPS jump and discarded. Prevents a teleporting
# point from being mistaken for a collision.
MAX_PLAUSIBLE_SPEED_KNOTS = 60.0

# Hard bounds for a usable position fix. AIS uses 91/181 as "not available".
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

# ---------------------------------------------------------------------------
# Spatial / temporal bucketing for the candidate self-join
# ---------------------------------------------------------------------------

# Width of a time bin in seconds. Class-A AIS transmits every 2-10 s while
# underway, so a 60 s bin contains several fixes per vessel.
TIME_BIN_SECONDS = 60

# Side length of a spatial grid cell in degrees (~1.1 km at this latitude).
# Must be larger than COLLISION_DISTANCE_M / 111_000 so that two vessels
# within the threshold always share at least one bucket.
GRID_CELL_DEG = 0.01

# Maximum time difference between two fixes in a matched bucket.
MAX_TIME_DIFF_SECONDS = 30

# Closest-approach distance (metres) below which a pair is a collision
# candidate. Normal traffic keeps hundreds of metres apart.
COLLISION_DISTANCE_M = 150.0

# ---------------------------------------------------------------------------
# Danger scoring weights
# ---------------------------------------------------------------------------

# Distance component
SCORE_DIST_CLOSE = 50  # <= 20 m
SCORE_DIST_MEDIUM = 30  # <= 50 m
SCORE_DIST_FAR = 10  # > 50 m (but <= COLLISION_DISTANCE_M)

# COG (course) change component — sudden turn indicates evasive action
SCORE_COG_LARGE = 40  # > 30 degrees
SCORE_COG_MEDIUM = 20  # > 15 degrees

# SOG (speed) change component
SCORE_SOG_DECEL = 30  # sudden deceleration (<= -2 kt) — emergency stop
SCORE_SOG_ACCEL = 15  # sudden acceleration (>= +2 kt) — evasive thrust

# Penalty for sparse AIS reporting (gap > 120 s reduces reliability)
SCORE_SPARSE_PENALTY = -30

# Bonus when a vessel's AIS track ends at the collision moment — strong
# indicator of capsize / sinking (Karin Høj signature)
SCORE_LAST_SIGNAL = 100

# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------

WINDOW_MINUTES = 10  # plot +/- this many minutes around collision
TOP_N_EVENTS = 5  # how many top-scoring pairs to report
EARTH_RADIUS_KM = 6371.0088
