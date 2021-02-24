#!/usr/bin/env python3

import subprocess
import sys
import time
from typing import Tuple

WAIT_BETWEEN_TRIES = 20
TRY_TIMES = 6
SUCCESS = "success"
RUNNING = "running"
FAILED = "failed"
STATUS_TABLE = {
    "PEND": RUNNING,
    "RUN": RUNNING,
    "DONE": SUCCESS,
    "PSUSP": RUNNING,
    "USUSP": RUNNING,
    "SSUSP": RUNNING,
    "WAIT": RUNNING,
    "EXIT": FAILED,
    "POST_DONE": SUCCESS,
    "POST_ERR": FAILED,
}


def query_status(jobid: int) -> Tuple[str, str, str]:
    try:
        proc = subprocess.run(
            f"bjobs -o stat -noheader {jobid}",
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf8",
        )
        stdout = proc.stdout
        stderr = proc.stderr
        stdout_is_empty = not stdout.strip()
        if stderr.startswith("Job <{}> is not found".format(jobid)) or stdout_is_empty:
            status_for_snakemake = "unknown"
        else:
            status_for_snakemake = STATUS_TABLE.get(stdout.strip(), "unknown")
    except subprocess.CalledProcessError:
        stdout = ""
        stderr = ""
        status_for_snakemake = "unknown"

    return stdout, stderr, status_for_snakemake


def main():
    jobid = int(sys.argv[1])

    stdout, stderr, status_for_snakemake = query_status(jobid)

    tries = 0
    while status_for_snakemake == "unknown" and tries < TRY_TIMES:
        time.sleep(WAIT_BETWEEN_TRIES)
        stdout, stderr, status_for_snakemake = query_status(jobid)
        tries += 1

    print(status_for_snakemake)


if __name__ == "__main__":
    main()
