"""
Run once from the btc-dashboard-api repository root after adding:
  collector.py
  shared/yf_core_cache.py

It patches snapshot_api.py, main.py and leading_routes.py for Phase 3.
"""

from pathlib import Path


def patch_snapshot_api(path: Path) -> None:
    text = path.read_text(encoding="utf-8")

    start_marker = "COLLECTOR_INTERVAL_SECONDS = max("
    end_marker = "\n\n@asynccontextmanager"

    if start_marker not in text:
        raise RuntimeError("snapshot_api.py: Phase 2 collector config marker not found")

    start = text.index(start_marker)
    end = text.index(end_marker, start)

    scheduler = """COLLECTOR_INTERVALS = {
    \"fast\": max(
        300,
        int(os.getenv(\"FAST_COLLECTOR_INTERVAL_SECONDS\", \"900\")),
    ),
    \"market\": max(
        900,
        int(os.getenv(\"MARKET_COLLECTOR_INTERVAL_SECONDS\", \"1800\")),
    ),
    \"hourly\": max(
        1800,
        int(os.getenv(\"HOURLY_COLLECTOR_INTERVAL_SECONDS\", \"3600\")),
    ),
    \"slow\": max(
        3600,
        int(os.getenv(\"SLOW_COLLECTOR_INTERVAL_SECONDS\", \"14400\")),
    ),
}

COLLECTOR_TIMEOUTS = {
    \"fast\": max(
        120,
        int(os.getenv(\"FAST_COLLECTOR_TIMEOUT_SECONDS\", \"600\")),
    ),
    \"market\": max(
        300,
        int(os.getenv(\"MARKET_COLLECTOR_TIMEOUT_SECONDS\", \"900\")),
    ),
    \"hourly\": max(
        300,
        int(os.getenv(\"HOURLY_COLLECTOR_TIMEOUT_SECONDS\", \"900\")),
    ),
    \"slow\": max(
        300,
        int(os.getenv(\"SLOW_COLLECTOR_TIMEOUT_SECONDS\", \"900\")),
    ),
}

COLLECTOR_RETRY_SECONDS = max(
    60,
    int(os.getenv(\"COLLECTOR_RETRY_SECONDS\", \"300\")),
)

DB_PATH = DATA_DIR / \"basis_history.db\"
STABLECOIN_DB_PATH = DATA_DIR / \"stablecoin_history.db\"
DOMINANCE_DB_PATH = DATA_DIR / \"btc_dominance_history.db\"
OVERRIDE_FILE = DATA_DIR / \"manual_overrides.json\"

JUDGMENT_FILE = Path(os.getenv(\"JUDGMENT_FILE\", \"judgment_log.json\"))
TRADELOG_FILE = Path(os.getenv(\"TRADELOG_FILE\", \"trade_log.json\"))
EXECUTION_FILE = Path(os.getenv(\"EXECUTION_FILE\", \"trade_execution.json\"))

_collector_lock = threading.Lock()
_stop_event = threading.Event()


def _run_collector(mode: str) -> bool:
    if not _collector_lock.acquire(blocking=False):
        print(f\"[snapshot_api] collector busy; skipping {mode}\")
        return False

    timeout = COLLECTOR_TIMEOUTS[mode]

    try:
        started = time.time()
        print(f\"[snapshot_api] starting {mode} collector subprocess\")
        completed = subprocess.run(
            [sys.executable, \"collector.py\", mode],
            cwd=str(Path(__file__).resolve().parent),
            timeout=timeout,
            check=False,
        )
        elapsed = time.time() - started
        print(
            f\"[snapshot_api] {mode} collector exited \"
            f\"code={completed.returncode} after {elapsed:.2f}s\"
        )
        return completed.returncode == 0
    except subprocess.TimeoutExpired:
        print(
            f\"[snapshot_api] {mode} collector exceeded \"
            f\"{timeout}s timeout\"
        )
        return False
    except Exception as exc:
        print(f\"[snapshot_api] {mode} collector launch error: {exc}\")
        return False
    finally:
        _collector_lock.release()


def _initial_delay(mode: str, interval: int) -> float:
    snapshot = load_snapshot()
    if isinstance(snapshot, dict):
        collections = snapshot.get(\"collections\", {})
        if isinstance(collections, dict):
            info = collections.get(mode, {})
            if isinstance(info, dict):
                last = info.get(\"generated_unix\")
                if isinstance(last, (int, float)):
                    age = max(0.0, time.time() - float(last))
                    return max(0.0, interval - age)

    return {\"fast\": 0.0, \"market\": 20.0, \"hourly\": 40.0, \"slow\": 60.0}[mode]


def _collector_loop() -> None:
    next_runs = {
        mode: time.monotonic() + _initial_delay(mode, interval)
        for mode, interval in COLLECTOR_INTERVALS.items()
    }

    print(
        \"[snapshot_api] collector schedule — \"
        + \", \".join(
            f\"{mode}={interval}s\"
            for mode, interval in COLLECTOR_INTERVALS.items()
        )
    )

    while not _stop_event.is_set():
        mode = min(next_runs, key=next_runs.get)
        delay = max(0.0, next_runs[mode] - time.monotonic())

        if _stop_event.wait(delay):
            return

        success = _run_collector(mode)
        interval = COLLECTOR_INTERVALS[mode]
        next_runs[mode] = time.monotonic() + (
            interval if success
            else min(COLLECTOR_RETRY_SECONDS, interval)
        )
"""

    text = text[:start] + scheduler + text[end:]

    old_health = '\"collector_interval_s\": COLLECTOR_INTERVAL_SECONDS,'
    new_health = '\"collector_intervals_s\": COLLECTOR_INTERVALS,'
    if old_health not in text:
        raise RuntimeError("snapshot_api.py: health interval marker not found")
    text = text.replace(old_health, new_health, 1)

    compile(text, str(path), "exec")
    path.write_text(text, encoding="utf-8")
    print("PATCHED snapshot_api.py")


def patch_main(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    old = "def fetch_cme_basis() -> dict:\n    from shared.yf_cache import get_series\n"
    new = "def fetch_cme_basis() -> dict:\n    from shared.yf_core_cache import get_series\n"

    if old not in text:
        if "from shared.yf_core_cache import get_series" in text:
            print("SKIP main.py — already using yf_core_cache")
            return
        raise RuntimeError("main.py: fetch_cme_basis import marker not found")

    text = text.replace(old, new, 1)
    compile(text, str(path), "exec")
    path.write_text(text, encoding="utf-8")
    print("PATCHED main.py")


def patch_leading(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    old = "from shared.yf_cache import get_series as _yf"
    new = "from shared.yf_core_cache import get_series as _yf"

    if old not in text:
        if new in text:
            print("SKIP leading_routes.py — already using yf_core_cache")
            return
        raise RuntimeError("leading_routes.py: shared yf import marker not found")

    text = text.replace(old, new, 1)
    compile(text, str(path), "exec")
    path.write_text(text, encoding="utf-8")
    print("PATCHED leading_routes.py")


def main() -> None:
    root = Path.cwd()
    required = [
        root / "snapshot_api.py",
        root / "main.py",
        root / "leading_routes.py",
        root / "collector.py",
        root / "shared" / "yf_core_cache.py",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing required files:\n" + "\n".join(missing))

    patch_snapshot_api(root / "snapshot_api.py")
    patch_main(root / "main.py")
    patch_leading(root / "leading_routes.py")

    for rel in (
        "collector.py",
        "snapshot_api.py",
        "main.py",
        "leading_routes.py",
        "shared/yf_core_cache.py",
    ):
        path = root / rel
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
        print(f"PASS syntax — {rel}")

    print("Phase 3 patch complete.")


if __name__ == "__main__":
    main()
