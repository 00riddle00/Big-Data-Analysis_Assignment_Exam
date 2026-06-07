<!-- vim: set ft=markdown tw=88 nu ai et ts=2 sw=2: -->

# Report: AIS Vessel Collision Detection

## 1. Problem and Approach

The task is to identify, within a month of raw AIS records, the two moving vessels
whose tracks intersect in space and time — a collision — inside a 50 nautical mile
radius of 55.225°N, 14.245°E (Bornholmsgat, Baltic Sea), and to do so with a
big-data engine rather than plain Pandas.

The pipeline is a classic funnel: cheap, highly selective filters run first so that
each later, more expensive stage processes as little data as possible:

```
ingest → validity → time window → geo (bbox → haversine) → GPS-jump removal
       → service vessel removal → moving-only → bucketed self-join → danger scoring
       → ranked results → Folium trajectory map
```

## 2. Data Engineering

**Ingestion.** The Danish daily files share a fixed header. Only the 11 columns
needed for analysis are selected and cast to proper types. Timestamps
(`dd/MM/yyyy HH:mm:ss`, UTC) are parsed to Spark timestamps. `PERMISSIVE` mode
turns malformed rows into nulls rather than crashing.

**Validity filtering.** AIS is messy. Rows with null timestamp or MMSI,
out-of-range coordinates, the `(0, 0)` null island, and the `91/181` not-available
sentinels are dropped. Only Class A and Class B mobile reports are kept — base
stations and navigational aids are discarded.

**Time window.** Restricted to `2021-12-01 00:00:00` – `2021-12-31 23:59:59` UTC.

## 3. Spatial Filtering (50 nm)

A bounding box pre-filter is applied first — a pure column comparison on latitude
and longitude derived from the radius — and only then is the exact Haversine
great-circle distance computed on the survivors. The bounding box discards the
vast majority of rows with an almost-free comparison, so the trigonometry runs on
a small fraction of the data. The Haversine is implemented entirely with Spark SQL
column functions (no Python UDF) so it runs vectorised inside the JVM.

After filtering: **26,312,825 fixes** remain inside the 50 nm radius.

## 4. Noise Filtering

### 4.1 GPS jumps / position anomalies

A single corrupt fix can teleport a vessel kilometres in one report, landing it
momentarily on top of another ship and faking a collision. For each MMSI ordered
by time, the implied speed to the previous fix is computed. Fixes implying more
than **60 knots** are physically impossible for a cargo vessel and are discarded
before the proximity search.

### 4.2 Stationary vessels

Two ships moored next to each other sit metres apart for hours and would otherwise
look like a permanent zero-distance collision. Fixes with **SOG < 1.0 knot** are
removed. Anchored and moored vessels report ~0 knots and are dropped entirely.

### 4.3 Service vessels

Rescue boats, coast-guard vessels (KBV), pilot boats, and SAR vessels that cluster
around an accident scene are removed by name. Without this filter they would appear
as false collision candidates near the impact coordinates.

## 5. Computational Strategy: Avoiding the Cartesian Product

The core difficulty is that finding close pairs is naturally an all-pairs O(N²)
problem — infeasible on a month of high-frequency AIS data.

Instead, every fix is assigned two bucket keys:

- a **time bin** = `floor(epoch_seconds / 60)`
- a **spatial grid cell** = `floor((lat − lat₀) / 0.01)`, `floor((lon − lon₀) / 0.01)`
  (~1.1 km cells)

Two vessels that physically touch must share a small region at the same instant, so
they must share a `(time_bin, cell)` bucket. A **self-join only within buckets**
reduces the comparison count from quadratic to roughly linear.

To guarantee no pair is missed at a cell or bin boundary, the left side of the join
is expanded into its 3×3×3 = 27 neighbouring buckets (a fixed constant blow-up)
and joined against the right side's single home bucket.

## 6. Danger Scoring

Pure proximity is not sufficient to distinguish a real collision from an
intentional close-quarters operation such as a tug escort or refuelling. The danger
score combines multiple signals:

| Signal         | Condition               | Points |
| -------------- | ----------------------- | ------ |
| Distance       | ≤ 20 m                  | +50    |
| Distance       | ≤ 50 m                  | +30    |
| Distance       | > 50 m                  | +10    |
| COG change     | > 30°                   | +40    |
| COG change     | > 15°                   | +20    |
| SOG change     | ≤ −2 kt (deceleration)  | +30    |
| SOG change     | ≥ +2 kt (acceleration)  | +15    |
| Sparse reports | gap > 120 s             | −30    |
| Last signal    | AIS track ends abruptly | +100   |

The **last-signal bonus** is the key discriminator. Karin Høj capsized immediately
after impact — her AIS track simply stops. This +100 bonus ensures a vessel that
goes silent at the encounter moment scores far higher than any routine
close-quarters operation where both vessels continue transmitting.

## 7. Findings

The pipeline identifies the documented Bornholmsgat collision between the British
general cargo ship **MV Scot Carrier** (MMSI 232018267) and the Danish split hopper
barge **Karin Høj** (MMSI 219021240) on **13 December 2021 at 02:27:29 UTC** near
**55.2231°N, 14.2437°E** — squarely inside the search area. Karin Høj capsized
shortly after impact; her AIS track ends abruptly, which is reflected in the
maximum danger score of **205**.

The Scot Carrier's telemetry shows a sharp course change (~35°) immediately before
impact — consistent with the MAIB accident report finding that the vessel turned
right while the officer of the watch was asleep.

## 8. Limitations

- All scoring thresholds are heuristics centralised in `config.py` and can be
  tuned without touching the processing logic.
- The pipeline processes all 31 days algorithmically and discovers the collision
  date from the data — December 13th is not hardcoded anywhere.
- The visualisation shows only the Folium trajectory map. SOG/COG telemetry
  charts are not generated but the data is available in `results.txt`.
