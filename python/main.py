#!/usr/bin/env python3
# ----------------------------------------------------------------------------
#
#	Delphix DCT Python SDK demo.
#
# Overview
# - Lists all VDBs with size and storage size.
# - Optional: refresh a VDB by name or ID (the API accepts name as ID).
#
# Environment
# - DCT_ENGINE_URL e.g. https://your-engine.example.com
# - DCT_API_KEY API key for DCT, include the required prefix (e.g. "apk ").
# - DCT_INSECURE set to "0" to enable SSL verification (default is insecure).
# - DCT_CA_CERT optional path to a CA bundle file.
#
# Example
#
#	export DCT_ENGINE_URL="https://your-engine.example.com"
#	export DCT_API_KEY="apk YOUR_API_KEY"
#
# Usage (uv)
#
#	uv run python main.py
#	uv run python main.py "prod01-copy01"  # name works as id
#
# Output (tab-separated)
#
#	name	size_bytes	storage_bytes
#	prod01-copy01	339292672	12345678
#
# ----------------------------------------------------------------------------

import os
import sys
import time
import urllib3
import delphix.api.gateway
from delphix.api.gateway.api.vdbs_api import VDBsApi
from delphix.api.gateway.api.jobs_api import JobsApi
from delphix.api.gateway.model.refresh_vdbby_timestamp_parameters import (
    RefreshVDBByTimestampParameters,
)


DEFAULT_PAGE_SIZE = 200
POLL_INTERVAL_SECONDS = 2.0
ANIM_INTERVAL_SECONDS = 0.2


# ----------------------------------------------------------------------------
# Main
# - Lists VDBs by default.
# - Refreshes a named VDB when an argument is provided.
# ----------------------------------------------------------------------------

def main() -> int:
    engine_url = must_env("DCT_ENGINE_URL")
    api_key = must_env("DCT_API_KEY")

    client = new_client(engine_url, api_key)
    vdbs_api = VDBsApi(client)
    jobs_api = JobsApi(client)

    print_config(engine_url, api_key)

    if len(sys.argv) > 1:
        vdb_ref = sys.argv[1].strip()
        if not vdb_ref:
            raise SystemExit("vdb name/id argument is empty")
        refresh_vdb_to_latest(vdbs_api, jobs_api, vdb_ref, vdb_ref)
        return 0

    print("name\tsize_bytes\tstorage_bytes")
    cursor = ""
    while True:
        if cursor:
            resp = vdbs_api.get_vdbs(limit=DEFAULT_PAGE_SIZE, cursor=cursor)
        else:
            resp = vdbs_api.get_vdbs(limit=DEFAULT_PAGE_SIZE)
        for vdb in (getattr(resp, "items", None) or []):
            name = getattr(vdb, "name", None) or ""
            size = int(vdb.size) if getattr(vdb, "size", None) is not None else 0
            storage = (
                int(vdb.storage_size) if getattr(vdb, "storage_size", None) is not None else 0
            )
            print(f"{name}\t{size}\t{storage}")

        cursor = getattr(getattr(resp, "response_metadata", None), "next_cursor", None) or ""
        if not cursor:
            break

    return 0


# ----------------------------------------------------------------------------
# Client
# - Builds the DCT SDK client.
# - Sets the server URL and Authorization header.
# ----------------------------------------------------------------------------

def new_client(engine_url: str, api_key: str) -> delphix.api.gateway.ApiClient:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    cfg = delphix.api.gateway.Configuration()
    cfg.host = engine_url.rstrip("/") + "/dct/v3"
    cfg.api_key["ApiKeyAuth"] = api_key
    cfg.verify_ssl = False
    if os.getenv("DCT_INSECURE", "").strip() in {"0", "false", "FALSE", "no", "NO"}:
        cfg.verify_ssl = True
    ca_cert = os.getenv("DCT_CA_CERT", "").strip()
    if ca_cert:
        cfg.ssl_ca_cert = ca_cert
    return delphix.api.gateway.ApiClient(cfg)


# ----------------------------------------------------------------------------
# Env
# - Reads required env vars.
# - Fails fast with a clear error.
# ----------------------------------------------------------------------------

def must_env(key: str) -> str:
    value = os.getenv(key, "").strip()
    if not value:
        raise SystemExit(f"missing required env var {key}")
    return value


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Config
# - Prints config for troubleshooting.
# - Masks secrets before printing.
# ----------------------------------------------------------------------------

def print_config(engine_url: str, api_key: str) -> None:
    masked = mask_secret(api_key)
    print(f"engine_url={engine_url}")
    print(f"api_key={masked}")


# ----------------------------------------------------------------------------
# Security
# - Masks secrets while keeping a short suffix for debugging.
# ----------------------------------------------------------------------------

def mask_secret(value: str) -> str:
    keep = 4
    if len(value) <= keep:
        return "*" * len(value)
    return "*" * (len(value) - keep) + value[-keep:]


# ----------------------------------------------------------------------------
# Refresh
# - Starts a refresh to the latest timestamp.
# - Waits for completion and prints elapsed time.
# ----------------------------------------------------------------------------

def refresh_vdb_to_latest(
    vdbs_api: VDBsApi,
    jobs_api: JobsApi,
    vdb_id: str,
    vdb_name: str,
) -> None:
    start = time.time()

    params = RefreshVDBByTimestampParameters()
    resp = vdbs_api.refresh_vdb_by_timestamp(
        vdb_id,
        refresh_vdbby_timestamp_parameters=params,
    )

    job = getattr(resp, "job", None)
    job_id = getattr(job, "id", None) or ""
    job_status = getattr(job, "status", None) or ""

    if job_id:
        print(f"refresh initiated for {vdb_name} (job_id={job_id} status={job_status})")
        final_status = wait_for_job(jobs_api, job_id)
        elapsed = int(time.time() - start)
        print(f"refresh finished for {vdb_name} (status={final_status} elapsed={elapsed}s)")
    else:
        print(f"refresh initiated for {vdb_name}")


# ----------------------------------------------------------------------------
# Jobs
# - Polls the job endpoint.
# - Stops at terminal status.
# ----------------------------------------------------------------------------

def wait_for_job(jobs_api: JobsApi, job_id: str) -> str:
    spinner = ["|", "/", "-", "\\"]
    spin_index = 0
    next_poll = 0.0

    while True:
        now = time.time()
        if now >= next_poll:
            job = jobs_api.get_job_by_id(job_id)
            status = getattr(job, "status", None) or ""
            if is_terminal_job_status(status):
                sys.stdout.write("\r")
                sys.stdout.write(f"waiting for job {job_id} status={status}\n")
                sys.stdout.flush()
                return status
            next_poll = now + POLL_INTERVAL_SECONDS

        status = getattr(job, "status", None) or "" if "job" in locals() else ""
        sys.stdout.write(f"\r{spinner[spin_index % len(spinner)]} waiting for job {job_id} status={status}")
        sys.stdout.flush()
        spin_index += 1
        time.sleep(ANIM_INTERVAL_SECONDS)


# ----------------------------------------------------------------------------
# Jobs
# - Determines if the job is finished.
# ----------------------------------------------------------------------------

def is_terminal_job_status(status: str) -> bool:
    status_upper = status.upper()
    return status_upper in {"COMPLETED", "FAILED", "CANCELED", "CANCELLED", "ABANDONED"}


if __name__ == "__main__":
    raise SystemExit(main())
