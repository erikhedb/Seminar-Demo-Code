#Requires -Version 5.1
<#
.SYNOPSIS
    PowerShell module for the Delphix Data Control Tower (DCT) REST API.

.DESCRIPTION
    Provides functions to connect to a DCT instance and perform common operations:
    - Connect and verify API key
    - Refresh a Virtual Database (VDB)
    - Snapshot a VDB
    - Execute a Masking Job
    - Trigger a Replication Profile
    - Wait for an async Job to complete
#>

# Module-level state
$script:DCTBaseUrl            = $null
$script:DCTHeaders             = $null
$script:DCTSkipCertificateCheck = $false

#region Internal helper

function Invoke-DCTRequest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Method,
        [Parameter(Mandatory)] [string] $Path,
        [object] $Body
    )

    if (-not $script:DCTBaseUrl -or -not $script:DCTHeaders) {
        throw 'Not connected to DCT. Call Connect-DelphixDCT first.'
    }

    $uri = $script:DCTBaseUrl + '/dct/v3' + $Path

    $params = @{
        Method                  = $Method
        Uri                     = $uri
        Headers                 = $script:DCTHeaders
        ContentType             = 'application/json'
        ErrorAction             = 'Stop'
        SkipCertificateCheck    = $script:DCTSkipCertificateCheck
    }

    if ($Body) {
        $params['Body'] = ($Body | ConvertTo-Json -Depth 10 -Compress)
    }

    try {
        $response = Invoke-RestMethod @params
        return $response
    }
    catch [System.Net.WebException] {
        $statusCode = [int]$_.Exception.Response.StatusCode
        $message    = $_.Exception.Message
        throw ('DCT API error [' + $statusCode + '] on ' + $Method + ' ' + $Path + ' : ' + $message)
    }
    catch {
        throw ('DCT request failed for ' + $Method + ' ' + $Path + ' : ' + $_)
    }
}

#endregion

#region Exported functions

function Connect-DelphixDCT {
    <#
    .SYNOPSIS
        Connects to a DCT instance and verifies the API key.

    .PARAMETER BaseUrl
        Base URL of the DCT host, e.g. https://my-dct-host (no trailing slash).

    .PARAMETER ApiKey
        The DCT API key. Sent as the Authorization header value.

    .OUTPUTS
        PSCustomObject with product info returned by the API.

    .PARAMETER SkipCertificateCheck
        Skip SSL certificate validation. Use for self-signed or lab certs.

    .EXAMPLE
        Connect-DelphixDCT -BaseUrl 'https://dct.example.com' -ApiKey 'apk-abc123'
        Connect-DelphixDCT -BaseUrl 'https://192.168.1.10' -ApiKey 'apk-abc123' -SkipCertificateCheck
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $BaseUrl,
        [Parameter(Mandatory)] [string] $ApiKey,
        [switch] $SkipCertificateCheck
    )

    $script:DCTBaseUrl              = $BaseUrl.TrimEnd('/')
    $script:DCTSkipCertificateCheck = $SkipCertificateCheck.IsPresent
    $script:DCTHeaders = @{
        Authorization = $ApiKey
        Accept        = 'application/json'
    }

    try {
        $info = Invoke-DCTRequest -Method GET -Path '/reporting/product_info'
        Write-Verbose ('Connected to DCT at ' + $BaseUrl)
        return $info
    }
    catch {
        $script:DCTBaseUrl = $null
        $script:DCTHeaders  = $null
        throw ('Failed to connect to DCT at ' + $BaseUrl + ': ' + $_)
    }
}

function Invoke-VDBRefresh {
    <#
    .SYNOPSIS
        Refreshes a VDB to the latest snapshot (or a specific snapshot).

    .PARAMETER VdbId
        The ID of the VDB to refresh.

    .PARAMETER SnapshotId
        Optional. The snapshot ID to refresh from. Omit to use the latest snapshot.

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        Invoke-VDBRefresh -VdbId 'vdb-123'
        Invoke-VDBRefresh -VdbId 'vdb-123' -SnapshotId 'snap-456'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $VdbId,
        [string] $SnapshotId
    )

    $body = @{}
    if ($SnapshotId) {
        $body['snapshot_id'] = $SnapshotId
    }

    $resp = Invoke-DCTRequest -Method POST -Path ('/vdbs/' + $VdbId + '/refresh_by_snapshot') -Body $body
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function New-VDBSnapshot {
    <#
    .SYNOPSIS
        Takes a snapshot of a VDB.

    .PARAMETER VdbId
        The ID of the VDB to snapshot.

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        New-VDBSnapshot -VdbId 'vdb-123'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $VdbId
    )

    $resp = Invoke-DCTRequest -Method POST -Path ('/vdbs/' + $VdbId + '/snapshots')
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function Invoke-MaskingJob {
    <#
    .SYNOPSIS
        Executes a Masking Job.

    .PARAMETER MaskingJobId
        The ID of the masking job to execute.

    .PARAMETER EngineId
        Optional. The engine ID to run the job on (standard jobs only).

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        Invoke-MaskingJob -MaskingJobId 'masking-job-789'
        Invoke-MaskingJob -MaskingJobId 'masking-job-789' -EngineId 'engine-1'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MaskingJobId,
        [string] $EngineId
    )

    $body = @{}
    if ($EngineId) {
        $body['engine_id'] = $EngineId
    }

    $resp = Invoke-DCTRequest -Method POST -Path ('/masking-jobs/' + $MaskingJobId + '/execute') -Body $body
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function Stop-VDB {
    <#
    .SYNOPSIS
        Stops a VDB.

    .PARAMETER VdbId
        The ID of the VDB to stop.

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        Stop-VDB -VdbId 'vdb-123'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $VdbId
    )

    $resp = Invoke-DCTRequest -Method POST -Path ('/vdbs/' + $VdbId + '/stop')
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function Start-VDB {
    <#
    .SYNOPSIS
        Starts a VDB.

    .PARAMETER VdbId
        The ID of the VDB to start.

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        Start-VDB -VdbId 'vdb-123'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $VdbId
    )

    $resp = Invoke-DCTRequest -Method POST -Path ('/vdbs/' + $VdbId + '/start')
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function Invoke-Replication {
    <#
    .SYNOPSIS
        Triggers execution of a Replication Profile.

    .PARAMETER ReplicationProfileId
        The ID of the replication profile to execute.

    .OUTPUTS
        Job object returned by the API.

    .EXAMPLE
        Invoke-Replication -ReplicationProfileId 'replication-profile-001'
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $ReplicationProfileId
    )

    $resp = Invoke-DCTRequest -Method POST -Path ('/replication-profiles/' + $ReplicationProfileId + '/execute')
    return $(if ($resp.job) { $resp.job } else { $resp })
}

function Wait-DCTJob {
    <#
    .SYNOPSIS
        Polls a DCT job every N seconds until it reaches a terminal state.

    .PARAMETER JobId
        The ID of the job to wait for.

    .PARAMETER PollIntervalSeconds
        How often to poll, in seconds. Default: 10.

    .PARAMETER TimeoutSeconds
        Maximum total wait time in seconds before throwing a timeout error. Default: 3600 (1 hour).

    .OUTPUTS
        The final Job object once a terminal state is reached.

    .EXAMPLE
        $job = Invoke-VDBRefresh -VdbId 'vdb-123'
        $result = Wait-DCTJob -JobId $job.id
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $JobId,
        [int] $PollIntervalSeconds = 10,
        [int] $TimeoutSeconds      = 3600
    )

    $terminalStatuses = @('COMPLETED', 'FAILED', 'TIMEDOUT', 'CANCELED', 'ABANDONED')
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    while ($true) {
        if ($stopwatch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
            throw ('Timed out after ' + $TimeoutSeconds + ' seconds waiting for job ' + $JobId + '.')
        }

        $job = Invoke-DCTRequest -Method GET -Path ('/jobs/' + $JobId)

        $pct  = if ($null -ne $job.percent_complete) { [string]$job.percent_complete + '%' } else { 'n/a' }
        $type = if ($job.localized_type) { ' [' + $job.localized_type + ']' } else { '' }
        Write-Host ('  [' + (Get-Date -Format 'HH:mm:ss') + ']' + $type + ' Job ' + $JobId + ' - ' + $job.status + ' (' + $pct + ')') -ForegroundColor DarkCyan

        if ($terminalStatuses -contains $job.status) {
            break
        }

        Start-Sleep -Seconds $PollIntervalSeconds
    }

    if ($job.status -ne 'COMPLETED') {
        $detail = if ($job.error_details) { ' Details: ' + $job.error_details } else { '' }
        throw ('Job ' + $JobId + ' ended with status ' + $job.status + '.' + $detail)
    }

    return $job
}

#endregion

Export-ModuleMember -Function Connect-DelphixDCT, Invoke-VDBRefresh, New-VDBSnapshot, Invoke-MaskingJob, Stop-VDB, Start-VDB, Invoke-Replication, Wait-DCTJob
