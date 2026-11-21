"""Local NNTile dashboard. Run from repository root: python dashboard_server.py"""
from __future__ import annotations

import json
import math
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'src'))
from nntile_sim.parser import parse_tasks
from nntile_sim.core.engine import Config, Simulator
from nntile_sim.core.global_planner import schedule as global_schedule

MIB = 1024 ** 2
POLICIES = {'lru-demand', 'belady-demand', 'global-planner'}
LOCK = threading.Lock()


def finite_positive(value, name, upper):
    number = float(value)
    if not math.isfinite(number) or not 0 < number <= upper:
        raise ValueError(f'{name}: ожидается число от 0 до {upper}')
    return number


def busy_time(events, lane):
    intervals = sorted((e.start_us, e.end_us) for e in events if e.lane == lane and e.end_us > e.start_us)
    total = 0.0
    left = right = None
    for a, b in intervals:
        if left is None:
            left, right = a, b
        elif a <= right:
            right = max(right, b)
        else:
            total += right - left
            left, right = a, b
    if left is not None:
        total += right - left
    return total


def run(payload):
    trace = payload.get('trace')
    allowed = {p.name: p for p in (ROOT / 'examples').glob('*.rec')}
    if trace not in allowed:
        raise ValueError('Неизвестная трасса')
    policies = payload.get('policies')
    if not isinstance(policies, list) or not policies or len(policies) != len(set(policies)) or any(p not in POLICIES for p in policies):
        raise ValueError('Выберите хотя бы один алгоритм')
    capacity = finite_positive(payload.get('capacity_mib'), 'GPU memory', 1_000_000)
    h2d = finite_positive(payload.get('h2d_gbps'), 'H2D', 100_000)
    d2h = finite_positive(payload.get('d2h_gbps'), 'D2H', 100_000)
    chunk = finite_positive(payload.get('chunk_mib'), 'Chunk', 1_000_000)
    if round(chunk * MIB) < 1:
        raise ValueError('Размер чанка меньше одного байта')
    order = payload.get('compute_order', 'topological')
    if order not in ('topological', 'trace'):
        raise ValueError('Неизвестный порядок Compute')
    tasks = parse_tasks(allowed[trace])
    if not tasks:
        raise ValueError('В трассе нет задач')
    results = []
    for policy in policies:
        cfg = Config(capacity_bytes=round(capacity * MIB),
                     h2d_bytes_per_us=h2d * 1000.0,
                     d2h_bytes_per_us=d2h * 1000.0,
                     compute_order=order,
                     memory_policy=policy if policy != 'global-planner' else 'lru-demand',
                     chunk_size_bytes=round(chunk * MIB) if policy == 'global-planner' else None,
                     prefetch_depth=0)
        try:
            result = global_schedule(tasks, cfg) if policy == 'global-planner' else Simulator(tasks, cfg).run()
            if result.completed_jobs != len(tasks) or result.peak_bytes > cfg.capacity_bytes:
                raise ValueError('Недопустимое или неполное расписание')
            events = [{'lane': e.lane, 'action': e.action, 'handle': e.handle,
                       'job_id': e.job_id, 'start_us': e.start_us,
                       'end_us': e.end_us, 'used_bytes_after': e.used_bytes_after}
                      for e in result.events]
            busy = {lane: busy_time(result.events, lane) for lane in ('Compute', 'H2D', 'D2H')}
            results.append({'policy': policy, 'status': 'ok', 'makespan_us': result.makespan_us,
                            'peak_bytes': result.peak_bytes, 'h2d_bytes': result.h2d_bytes,
                            'd2h_bytes': result.d2h_bytes, 'completed_jobs': result.completed_jobs,
                            'busy_us': busy, 'compute_idle_us': max(0.0, result.makespan_us - busy['Compute']),
                            'events': events})
        except (ValueError, RuntimeError, AssertionError) as exc:
            results.append({'policy': policy, 'status': 'error', 'error': str(exc)})
    return {'trace': trace, 'tasks': len(tasks), 'capacity_mib': capacity, 'results': results}


class Handler(BaseHTTPRequestHandler):
    def send_json(self, data, status=200):
        raw = json.dumps(data, ensure_ascii=False, allow_nan=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == '/api/traces':
            self.send_json({'traces': sorted(p.name for p in (ROOT / 'examples').glob('*.rec'))})
        elif self.path in ('/', '/nntile-dashboard.html'):
            raw = (ROOT / 'nntile-dashboard.html').read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != '/api/run':
            self.send_error(404)
            return
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if length <= 0 or length > 100_000:
                raise ValueError('Неверный размер запроса')
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError('Ожидался JSON-объект')
            if not LOCK.acquire(blocking=False):
                self.send_json({'error': 'Симуляция уже выполняется'}, 409)
                return
            try:
                data = run(payload)
            finally:
                LOCK.release()
            self.send_json(data)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            self.send_json({'error': str(exc)}, 400)
        except Exception as exc:
            self.send_json({'error': f'{type(exc).__name__}: {exc}'}, 500)


if __name__ == '__main__':
    server = ThreadingHTTPServer(('127.0.0.1', 8765), Handler)
    print('NNTile Dashboard: http://127.0.0.1:8765')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
