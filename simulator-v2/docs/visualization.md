# Plotly Visualization

The simulator creates interactive HTML visualizations
of Compute, H2D, and D2H events.

## Generate a plot during simulation

From the simulator-v2 directory:

    nntile-sim ../examples/tasks_mbs1.rec \
      --gpu-memory-mib 1349 \
      --h2d-gbps 12 \
      --d2h-gbps 12 \
      --memory-policy lru-demand \
      --events-csv results/events_lru_1349.csv \
      --plot-html results/events_lru_1349.html

## Plot an existing CSV

    nntile-plot results/events_lru_1349.csv \
      --output results/events_lru_1349.html

Existing CSV files can be visualized without rerunning
the simulation.

## Overview

The overview shows the fraction of time each resource lane
is active within equal-sized time bins.

Compute, H2D, and D2H are measured independently,
so overlapping activity is possible.

The number of bins can be adjusted with:

    --overview-bins 1000

## Detailed timeline

The detailed timeline displays original event intervals.

Features:
- Interactive zoom, pan, and hover
- Initial focus on the largest Compute stall
- Navigation to the first 200 ms
- Navigation to the full simulation

Custom initial window:

    --window-ms 500

## Compute stalls

Compute stalls are gaps between consecutive Compute events.
A separate chart shows their start times and durations.

## GPU memory occupancy

Memory occupancy is not currently plotted.

The used_bytes_after field does not provide a complete
chronological history of memory allocation and release.

Accurate occupancy plots will require explicit timestamped
memory allocation and release events.

Generated HTML files contain Plotly JavaScript and can
be opened independently in a browser.
