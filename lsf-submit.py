#!/usr/bin/env python3
"""
lsf-submit.py

Script to wrap bsub sync command for Snakemake. Uses the following job or
cluster parameters:

+ `threads`
+ `resources`
    - `mem_mb`: Expected memory requirements in megabytes. Overrides
      cluster.mem_mb
+ `cluster`
    - `mem_mb`: Memory that will be requested for the cluster for the job.
      Overriden by resources.mem_mb, if present.
      `resources`
    - `queue`: Which queue to run job on
    - `logdir`: Where to log stdout/stderr from cluster command
    - `output`: Name of stdout logfile
    - `error`: Name of stderr logfile
    - `jobname`: Job name (with wildcards)

Author: Michael B Hall
Adapted from: https://github.com/jaicher/snakemake-sync-bq-sub
"""

import sys
import re
import math
import subprocess
import time
from pathlib import Path
from snakemake.utils import read_job_properties


DEFAULT_NAME = "jobname"


def generate_resources_command(job_properties: dict) -> str:
    threads = job_properties.get("threads", int(1))
    resources = job_properties.get("resources", dict())
    mem_mb = resources.get(
        "mem_mb", cluster.get("mem_mb", int(4000))
    )
    walltime_min = resources.get(
            "walltime_min", int(240)
    )
    walltime_hhmm = '{hours:02.0f}:{minutes:02.0f}'.format(
            hours=math.floor(walltime_min / 60), minutes=walltime_min % 60
    )
    return (
        "-M {mem_mb} -n {threads} -W {walltime_hhmm}"
        " -R 'select[mem>{mem_mb}] rusage[mem={mem_mb}] span[hosts=1]'"
        " -m compute-nx360"
    ).format(mem_mb=mem_mb, threads=threads, walltime_hhmm=walltime_hhmm)


def get_job_name(job_properties: dict) -> str:
    """Get the group or rule name. If neither exists, use the DEFAULT_NAME
    NOTE: if group is present rule is not valid therefore group must come before rule
    """
    if job_properties.get("type", "") == "group":
        groupid = job_properties.get("groupid", "group")
        jobid = job_properties.get("jobid", "").split("-")[0]
        jobname = "{groupid}_{jobid}".format(groupid=groupid, jobid=jobid)
    else:
        wildcards = job_properties.get("wildcards", dict())
        wildcards_str = (
            ".".join("{}={}".format(k, v) for k, v in wildcards.items()) or "unique"
        )
        name = job_properties.get("rule", "") or DEFAULT_NAME
        jobname = cluster.get("jobname", "{0}.{1}".format(name, wildcards_str))

    return jobname


def generate_jobinfo_command(job_properties: dict) -> str:
    log_dir = Path(cluster.get("logdir", "/home/kraemers/temp/logs2"))

    if not log_dir.absolute().exists():
        raise NotADirectoryError(
            "Log directory does not exist: {}".format(str(log_dir.absolute()))
        )

    jobname = get_job_name(job_properties)
    out_log = str(log_dir / cluster.get("output", "{}.out".format(jobname)))
    err_log = str(log_dir / cluster.get("error", "{}.err".format(jobname)))

    return '-o "{out_log}" -e "{err_log}" -J "{jobname}"'.format(
        out_log=out_log, err_log=err_log, jobname=jobname
    )


jobscript = sys.argv[-1]
job_properties = read_job_properties(jobscript)
cluster = job_properties.get("cluster", dict())

jobinfo_cmd = generate_jobinfo_command(job_properties)

resources_cmd = generate_resources_command(job_properties)

queue = cluster.get("queue", "")
queue_cmd = "-q {}".format(queue) if queue else ""

cluster_cmd = " ".join(sys.argv[1:-1])

# command to submit to cluster
submit_cmd = "bsub {resources} {job_info} {queue} {cluster} {jobscript}".format(
    resources=resources_cmd,
    job_info=jobinfo_cmd,
    queue=queue_cmd,
    cluster=cluster_cmd,
    jobscript=jobscript,
)


try:
    response = subprocess.run(
        submit_cmd, check=True, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, encoding='utf8',
            executable='/bin/bash'
    )
except subprocess.CalledProcessError as error:
    raise error

# Get jobid
response_stdout = response.stdout

# match = re.search(r"Job <(\d+)> is submitted", response_stdout)
# if match:
#     time.sleep(5)
#     print(match.group(1))
# else:
#     print(12341234)

try:
    match = re.search(r"Job <(\d+)> is submitted", response_stdout)
    jobid = match.group(1)
    print(jobid)
except Exception as error:
    raise error
