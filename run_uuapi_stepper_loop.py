import os
import subprocess
import sys
import threading
import time
from pathlib import Path


WORKDIR = Path(__file__).resolve().parent
VENV_PYTHON = WORKDIR / ".venv" / "Scripts" / "python.exe"
PYTHON_EXE = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)
TARGET_SCRIPT = WORKDIR / "uuapi_stepper.py"
STOP_COMMANDS = {"stop", "quit", "exit", "停止"}
DEFAULT_WORKER_COUNT = max(1, min(os.cpu_count() or 1, 3))
WORKER_COUNT = max(1, int(os.environ.get("UUAPI_WORKERS", str(DEFAULT_WORKER_COUNT))))
LOOP_DELAY_SECONDS = float(os.environ.get("UUAPI_LOOP_DELAY_SECONDS", "1.0"))
STOP_POLL_INTERVAL_SECONDS = 0.2


def log(message: str) -> None:
    print(message, flush=True)


def start_input_listener(stop_event: threading.Event, stop_state: dict[str, str]) -> None:
    def _reader() -> None:
        while not stop_event.is_set():
            try:
                user_input = input().strip().lower()
            except EOFError:
                stop_state["reason"] = "eof"
                stop_event.set()
                return

            if user_input in STOP_COMMANDS:
                stop_state["reason"] = user_input
                stop_event.set()
                return

    threading.Thread(target=_reader, daemon=True).start()


def sleep_until_next_round(stop_event: threading.Event) -> None:
    deadline = time.monotonic() + LOOP_DELAY_SECONDS
    while not stop_event.is_set() and time.monotonic() < deadline:
        time.sleep(STOP_POLL_INTERVAL_SECONDS)


def worker_loop(worker_id: int, stop_event: threading.Event) -> None:
    run_count = 0
    env = os.environ.copy()
    env["UUAPI_WORKER_ID"] = str(worker_id)

    while not stop_event.is_set():
        run_count += 1
        prefix = f"[worker-{worker_id} run-{run_count}]"
        log(f"{prefix} starting {TARGET_SCRIPT.name}")

        result = subprocess.run(
            [str(PYTHON_EXE), str(TARGET_SCRIPT)],
            cwd=str(WORKDIR),
            check=False,
            env=env,
        )

        if result.returncode == 0:
            log(f"{prefix} completed successfully")
        else:
            log(f"{prefix} failed with exit code {result.returncode}")

        if stop_event.is_set():
            break

        sleep_until_next_round(stop_event)

    log(f"[worker-{worker_id}] stopped")


def main() -> None:
    if not TARGET_SCRIPT.exists():
        raise FileNotFoundError(f"Target script not found: {TARGET_SCRIPT}")

    stop_event = threading.Event()
    stop_state = {"reason": ""}
    start_input_listener(stop_event, stop_state)

    log("Parallel loop started.")
    log(f"Target script: {TARGET_SCRIPT}")
    log(f"Python: {PYTHON_EXE}")
    log(f"Workers: {WORKER_COUNT}")
    log("Type stop, quit, exit, or 停止 to stop after current runs finish.")

    workers: list[threading.Thread] = []
    for worker_id in range(1, WORKER_COUNT + 1):
        worker = threading.Thread(
            target=worker_loop,
            args=(worker_id, stop_event),
            daemon=True,
            name=f"uuapi-worker-{worker_id}",
        )
        worker.start()
        workers.append(worker)

    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        stop_state["reason"] = "keyboardinterrupt"
        stop_event.set()
        log("Stop requested by keyboard interrupt. Waiting for current runs to finish.")
        for worker in workers:
            worker.join()

    if stop_state["reason"]:
        log(f"Stopped by: {stop_state['reason']}")
    else:
        log("All workers exited.")


if __name__ == "__main__":
    main()
