# Delphix DCT PowerShell Demo

A PowerShell demo that automates a typical Delphix Data Control Tower (DCT) workflow: stop a VDB, refresh it, start it back up, run a masking job, and take a snapshot.

## Prerequisites

- PowerShell 5.1 or later (Windows PowerShell or [PowerShell 7+](https://github.com/PowerShell/PowerShell))
- Network access to your DCT instance
- A valid DCT API key

## Quick Start

1. **Set your API key** as an environment variable:

   ```powershell
   # PowerShell
   $env:DCT_API_KEY = 'apk 2.your-api-key-here'
   ```

   ```bash
   # Bash / Zsh
   export DCT_API_KEY='apk 2.your-api-key-here'
   ```

2. **Edit the configuration block** at the top of `demo.ps1` with your environment-specific values:

   | Variable               | Description                              |
   | ---------------------- | ---------------------------------------- |
   | `$BaseUrl`             | DCT instance URL (e.g. `https://x.x.x.x`) |
   | `$VdbId`               | ID of the VDB to operate on              |
   | `$MaskingJobId`        | UUID of the masking job to execute       |
   | `$EngineId`            | Masking engine ID                        |
   | `$ReplicationProfileId`| Replication profile ID (currently unused)|

3. **Run the script:**

   ```powershell
   pwsh ./powershell/demo.ps1
   ```

## Architecture

The project consists of two files:

```
powershell/
  DelphixDCT.psm1   # Reusable PowerShell module wrapping the DCT REST API
  demo.ps1           # Demo script that orchestrates a full workflow
```

### DelphixDCT.psm1 — API Module

A PowerShell module that wraps the DCT v3 REST API. It manages connection state at the module level and exposes the following functions:

| Function              | API Endpoint                                    | Description                        |
| --------------------- | ----------------------------------------------- | ---------------------------------- |
| `Connect-DelphixDCT`  | `GET /reporting/product_info`                   | Authenticate and verify connection |
| `Stop-VDB`            | `POST /vdbs/{id}/stop`                          | Stop a VDB                         |
| `Start-VDB`           | `POST /vdbs/{id}/start`                         | Start a VDB                        |
| `Invoke-VDBRefresh`   | `POST /vdbs/{id}/refresh_by_snapshot`           | Refresh a VDB from a snapshot      |
| `New-VDBSnapshot`     | `POST /vdbs/{id}/snapshots`                     | Take a new VDB snapshot            |
| `Invoke-MaskingJob`   | `POST /masking-jobs/{id}/execute`               | Execute a masking job              |
| `Invoke-Replication`  | `POST /replication-profiles/{id}/execute`       | Trigger replication                |
| `Wait-DCTJob`         | `GET /jobs/{id}` (polling)                      | Poll a job until completion        |

All API calls go through an internal `Invoke-DCTRequest` helper that handles URL construction, JSON serialization, error handling, and optional SSL certificate skipping.

### demo.ps1 — Workflow Script

Orchestrates a sequential pipeline of DCT operations:

```
Connect --> Refresh VDB --> Stop VDB --> Start VDB --> Masking Job --> Snapshot VDB
```

Each step:
1. Initiates an async operation via the module
2. Receives a job object
3. Polls `Wait-DCTJob` until the job reaches a terminal state
4. Exits with an error if any step fails

A `Write-Log` helper provides color-coded, timestamped console output (INFO/WARN/ERROR).

> **Note:** The replication step (step 7) is present but commented out. Uncomment it in `demo.ps1` if you want to include replication in the workflow.
