"""Interactive Plotly overview and raw event timeline for NNTile CSVs."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

LANES = ('Compute', 'H2D', 'D2H')
COLORS = {'Compute': '#2ca02c', 'H2D': '#1f77b4', 'D2H': '#ff7f0e'}


def read_events(path: str | Path) -> list[dict]:
    with Path(path).open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        required = {'lane', 'action', 'handle', 'job_id', 'start_us', 'end_us'}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f'Events CSV must contain {sorted(required)}')
        result = []
        for row in reader:
            start, end = float(row['start_us']), float(row['end_us'])
            if not (0 <= start <= end < float('inf')):
                raise ValueError('Invalid event start_us/end_us')
            result.append({**row, 'start_us': start, 'end_us': end})
    return result


def compute_gaps(events: list[dict]) -> list[tuple[float, float]]:
    spans = sorted((e['start_us'], e['end_us']) for e in events if e['lane'] == 'Compute')
    gaps, last = [], 0.0
    for start, end in spans:
        if start > last + 1e-7:
            gaps.append((last, start))
        last = max(last, end)
    return gaps


def default_window(events: list[dict], window_ms: float = 200) -> tuple[float, float]:
    if window_ms <= 0:
        raise ValueError('window_ms must be positive')
    end = max((e['end_us'] for e in events), default=0) / 1e6
    gaps = compute_gaps(events)
    center = (sum(max(gaps, key=lambda g: g[1] - g[0])) / 2e6) if gaps else 0
    width = window_ms / 1000
    start = max(0, center - width / 2)
    return start, max(start + 1e-9, min(end, start + width))


def aggregate_utilization(events: list[dict], bins: int = 500):
    """Per-bin lane busy fractions, using interval unions (not event counts)."""
    if bins <= 0:
        raise ValueError('bins must be positive')
    end = max((e['end_us'] for e in events), default=0)
    width = max(end / bins, 1.0)
    centers = [(i + 0.5) * width / 1e6 for i in range(bins)]
    result = {}
    for lane in LANES:
        spans = sorted((e['start_us'], e['end_us']) for e in events
                       if e['lane'] == lane and e['end_us'] > e['start_us'])
        # Union overlapping intervals so utilization never exceeds 100%.
        merged = []
        for a, b in spans:
            if merged and a <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
            else:
                merged.append((a, b))
        busy = [0.0] * bins
        for a, b in merged:
            first = max(0, min(bins - 1, int(a / width)))
            last = max(0, min(bins - 1, int((b - 1e-9) / width)))
            for i in range(first, last + 1):
                busy[i] += max(0.0, min(b, (i + 1) * width) - max(a, i * width))
        result[lane] = [min(100.0, max(0.0, x / width * 100)) for x in busy]
    return centers, result


def create_timeline(events: list[dict], output: str | Path,
                    title: str = 'NNTile simulation timeline',
                    window_ms: float = 200.0, overview_bins: int = 500) -> Path:
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as exc:
        raise RuntimeError('Plotly is required: python -m pip install plotly') from exc

    centers, utilization = aggregate_utilization(events, overview_bins)
    fig = make_subplots(rows=3, cols=1, shared_xaxes=False,
                        row_heights=[0.24, 0.49, 0.27], vertical_spacing=0.13,
                        subplot_titles=('Overview — aggregated lane utilization',
                                        'Detailed timeline — raw simulation events',
                                        'Compute stalls — start time vs duration'))
    for lane in LANES:
        fig.add_trace(go.Scattergl(
            x=centers, y=utilization[lane], mode='lines', name=lane,
            legendgroup=lane, line=dict(color=COLORS[lane], width=1.7),
            hovertemplate=f'{lane}<br>Time: %{{x:.3f}} s<br>Busy: %{{y:.2f}}%<extra></extra>',
        ), row=1, col=1)

    for lane in LANES:
        selected = [e for e in events if e['lane'] == lane and e['end_us'] > e['start_us']]
        if not selected:
            continue
        fig.add_trace(go.Bar(
            x=[(e['end_us'] - e['start_us']) / 1e6 for e in selected],
            base=[e['start_us'] / 1e6 for e in selected],
            y=[lane] * len(selected), orientation='h', name=lane,
            legendgroup=lane, showlegend=False,
            marker_color=COLORS[lane], marker_line_width=0,
            customdata=[[e.get('action', ''), e.get('job_id', ''), e.get('handle', ''),
                         e['start_us']/1e6, e['end_us']/1e6,
                         (e['end_us']-e['start_us'])/1e3] for e in selected],
            hovertemplate=('Action: %{customdata[0]}<br>JobId: %{customdata[1]}'
                           '<br>Handle: %{customdata[2]}<br>Start: %{customdata[3]:.6f} s'
                           '<br>End: %{customdata[4]:.6f} s'
                           '<br>Duration: %{customdata[5]:.3f} ms<extra></extra>'),
        ), row=2, col=1)

    gaps = compute_gaps(events)
    if gaps:
        fig.add_trace(go.Scattergl(
            x=[a/1e6 for a, _ in gaps], y=[(b-a)/1e3 for a, b in gaps],
            mode='markers', name='Compute idle', marker=dict(size=5, color='#d62728', opacity=.65),
            customdata=[[b/1e6] for _, b in gaps],
            hovertemplate=('Idle start: %{x:.6f} s<br>Idle end: %{customdata[0]:.6f} s'
                           '<br>Duration: %{y:.3f} ms<extra></extra>'),
        ), row=3, col=1)

    end = max((e['end_us'] for e in events), default=0) / 1e6
    initial = default_window(events, window_ms)
    fig.update_xaxes(title_text='Time (s)', row=1, col=1)
    fig.update_xaxes(title_text='Time (s)', range=list(initial), row=2, col=1)
    fig.update_xaxes(title_text='Stall start time (s)', range=[0, max(end, 1e-9)], row=3, col=1)
    fig.update_yaxes(title_text='Busy (%)', range=[0, 101], row=1, col=1)
    fig.update_yaxes(title_text='Lane', categoryorder='array',
                     categoryarray=['D2H', 'H2D', 'Compute'], row=2, col=1)
    fig.update_yaxes(title_text='Idle (ms)', rangemode='tozero', row=3, col=1)
    fig.update_layout(
        title=dict(text=title, x=.04, y=.985, yanchor='top'),
        height=1080, template='plotly_white', barmode='overlay', hovermode='closest',
        legend=dict(orientation='h', y=1.045, x=0.72, xanchor='left'),
        margin=dict(t=205, b=75, l=80, r=35),
        updatemenus=[dict(type='buttons', direction='right', x=0, y=1.105,
                          xanchor='left', yanchor='bottom', buttons=[
            dict(label='Largest stall (zoom)', method='relayout',
                 args=[{'xaxis2.range': list(initial)}]),
            dict(label='First 200 ms', method='relayout',
                 args=[{'xaxis2.range': [0, min(end, .2)]}]),
            dict(label='Full run', method='relayout',
                 args=[{'xaxis2.range': [0, max(end, 1e-9)]}]),
        ])],
    )
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output), include_plotlyjs=True, full_html=True)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Plot NNTile simulation events')
    parser.add_argument('events_csv', type=Path)
    parser.add_argument('--output', type=Path, help='Output HTML (default: <events>.html)')
    parser.add_argument('--window-ms', type=float, default=200.0)
    parser.add_argument('--overview-bins', type=int, default=500)
    args = parser.parse_args(argv)
    output = args.output or args.events_csv.with_suffix('.html')
    try:
        create_timeline(read_events(args.events_csv), output,
                        title=args.events_csv.stem, window_ms=args.window_ms,
                        overview_bins=args.overview_bins)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f'Plot failed: {exc}\n')
    print(f'Plotly timeline: {output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
