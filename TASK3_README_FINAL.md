# Task 3 Code Guide

## Files

-   `processor_updated.py` --- runs the full Task 3 pipeline: map-reduce
    timeline building, anomaly detection, DFSI calculation, terminal
    summary, and result export.
-   `anomalies_updated.py` --- contains the anomaly detectors for A, B,
    C, and D, plus helper summaries and DFSI support logic.
-   `partitioner.py` --- low-memory streaming partitioner used by the
    processor. It was kept unchanged.

------------------------------------------------------------------------

## What Task 3 does

Task 3 detects four types of suspicious maritime behavior from
reconstructed vessel timelines:

-   **A --- Going Dark**\
    AIS gap \> 4 hours where the vessel likely continued moving.

-   **B --- Loitering / Transfers**\
    Two vessels within 500 m, both below 1 knot, for more than 2 hours.

-   **C --- Draft Change at Sea**\
    Draught changes by more than 5% during a blackout \> 2 hours.

-   **D --- Identity Cloning / Teleportation**\
    Same MMSI appears in locations requiring impossible speed (\> 60
    knots).

------------------------------------------------------------------------

## DFSI Calculation

DFSI = (Max Gap Hours / 2) + (Total Impossible Distance NM / 10) + (C \*
15)

------------------------------------------------------------------------

## Run Task 3

Basic run:

``` bash
python processor_updated.py aisdk-2025-06-09.csv
```

Custom chunk size:

``` bash
python processor_updated.py aisdk-2025-06-09.csv 10000
```

With workers and output folder:

``` bash
python processor_updated.py aisdk-2025-06-09.csv 50000 8 results
```

------------------------------------------------------------------------

## Terminal Output

The script prints: - pipeline runtime - summary of anomalies A--D - top
vessels ranked by DFSI

Detailed results are saved to files to avoid excessive terminal output.

------------------------------------------------------------------------

## Output Files

-   `anomaly_a.json`
-   `anomaly_b.json`
-   `anomaly_c.json`
-   `anomaly_d.json`
-   `dfsi_ranked.json`
-   `dfsi_ranked.csv`

------------------------------------------------------------------------

##  Notes on Anomaly B

Assignment constraints were preserved: - distance ≤ 500 m - speed \< 1
knot - duration \> 2 hours

Additional improvements: - excluded `Moored` and `At anchor` - averaged
positions per time bucket - stricter distance filtering - allowed one
missing bucket

------------------------------------------------------------------------

# Results Interpretation

## Anomaly A --- Going Dark

\~78 vessels, \~79 events\
Low frequency but strong indicator of suspicious AIS shutdown.

## Anomaly B --- Loitering / Transfers

\~13,000 events\
Reduced from \~25,000 → filtering improved precision\
Still noisy due to dense maritime traffic.

## Anomaly C --- Draft Change

0 events\
Data limitation (missing/unreliable draught values).

## Anomaly D --- Teleportation

\~43 vessels, \~92 events\
Strongest anomaly → indicates spoofing / identity cloning.

## DFSI Interpretation

Dominated by anomaly D → teleportation is the strongest signal.

------------------------------------------------------------------------

##  Final Conclusion

-   Pipeline works correctly and consistently
-   Anomaly D is the most reliable signal
-   Anomaly B remains noisy but improved
-   Memory-efficient and scalable approach

> Key insight: Teleportation (identity cloning) is the dominant
> suspicious behavior in this dataset.
