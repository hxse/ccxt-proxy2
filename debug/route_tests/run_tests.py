import pytest
import os
import sys
from datetime import datetime

# Ensure we are in root logic
if os.getcwd() not in sys.path:
    sys.path.append(os.getcwd())


def run_tests_and_report():
    if os.getenv("CCXT_STATEFUL_DEBUG") != "1":
        raise SystemExit("stateful route report requires CCXT_STATEFUL_DEBUG=1")
    print("Running Route Tests...")

    # Define report path
    report_path = os.path.join("debug", "route_tests", "TEST_REPORT.md")

    # Run pytest and capture output
    # -v: verbose
    # -ra: show extra test summary info
    args = ["-v", "-ra", "debug/route_tests"]

    from _pytest.main import ExitCode

    # We can't easily capture pytest standard output via main() unless we redirect stdout
    # So we use subprocess for cleaner capture
    import subprocess

    result = subprocess.run(
        ["uv", "run", "pytest"] + args, capture_output=True, text=True
    )

    stdout = result.stdout
    stderr = result.stderr
    exit_code = result.returncode

    # Generate Markdown Report
    status_icon = "✅" if exit_code == 0 else "❌"

    report_content = f"""# Route Test Report (Sandbox)

**Status**: {status_icon} {"PASS" if exit_code == 0 else "FAIL"}
**Date**: {datetime.now().astimezone().isoformat(timespec="seconds")}

## Execution Log
```
{stdout}
```

## Error Details
```
{stderr}
```
"""

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Report generated at {report_path}")
    print(stdout)


if __name__ == "__main__":
    run_tests_and_report()
