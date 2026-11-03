import csv
import subprocess
import sys
from pathlib import Path


VALIDATOR = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "validate_chunk_events.py"
)


def test_validator_rejects_drop_of_nonresident_chunk(tmp_path):
    csv_path = tmp_path / "invalid_events.csv"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow((
            "lane", "action", "handle", "job_id",
            "start_us", "end_us", "used_bytes_after",
        ))
        writer.writerow((
            "Memory", "DROP", "A:0", "",
            0.0, 0.0, 0,
        ))

    result = subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            str(csv_path),
            "--capacity-mib", "8",
            "--chunk-mib", "4",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "Dropping nonresident chunk" in result.stderr