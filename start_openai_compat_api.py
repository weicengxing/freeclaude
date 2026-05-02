import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUT_LOG = ROOT / "uvicorn.out.log"
ERR_LOG = ROOT / "uvicorn.err.log"


def main() -> None:
    creationflags = 0
    if sys.platform.startswith("win"):
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    out = OUT_LOG.open("ab")
    err = ERR_LOG.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "openai_compat_api:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8001",
        ],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=out,
        stderr=err,
        creationflags=creationflags,
        close_fds=True,
    )
    print(process.pid)


if __name__ == "__main__":
    main()
