#Requires -Version 5.1
<#
.SYNOPSIS
    Demo script for the DelphixDCT PowerShell module.

.DESCRIPTION
    Demonstrates: Connect -> Refresh VDB -> Execute Masking Job -> Snapshot VDB -> Trigger Replication.

    Required environment variable:
        DCT_API_KEY  - your DCT API key

    Edit the configuration block below for the remaining settings.
#>

# ============================================================
# CONFIGURATION - update these values before running
# ============================================================
$BaseUrl              = 'https://URL'
$VdbId                = 'VDB-NAME'
$MaskingJobId         = '89c67404-6c9b-4878-a000-0fc569a20XXXX'
$EngineId             = 'ENGINE_ID'
$ReplicationProfileId = 'replication-profile-XXX'
# ============================================================

$ApiKey = $env:DCT_API_KEY
if (-not $ApiKey) {
    Write-Error 'No API key found. Export DCT_API_KEY before running this script.'
    exit 1
}

Import-Module "$PSScriptRoot/DelphixDCT.psm1" -Force

# ------------------------------------------------------------
# Logging helper
# ------------------------------------------------------------
function Write-Log {
    param(
        [Parameter(Mandatory)] [string] $Message,
        [ValidateSet('INFO', 'WARN', 'ERROR')] [string] $Level = 'INFO'
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $prefix    = '[' + $timestamp + '] [' + $Level + ']'

    $color = switch ($Level) {
        'INFO'  { 'Cyan' }
        'WARN'  { 'Yellow' }
        'ERROR' { 'Red' }
    }

    Write-Host ($prefix + ' ' + $Message) -ForegroundColor $color
}

# ------------------------------------------------------------
# 1. Connect and verify API key
# ------------------------------------------------------------
Write-Log ('Connecting to DCT at ' + $BaseUrl + ' ...')
try {
    $productInfo = Connect-DelphixDCT -BaseUrl $BaseUrl -ApiKey $ApiKey -SkipCertificateCheck
    Write-Log ('Connected successfully. Product: ' + ($productInfo | ConvertTo-Json -Compress))
}
catch {
    Write-Log ('Connection failed: ' + $_) -Level ERROR
    exit 1
}



# ------------------------------------------------------------
# 2. Refresh VDB
# ------------------------------------------------------------
Write-Log ('Refreshing VDB ' + $VdbId + ' to latest snapshot ...')
try {
    $refreshJob = Invoke-VDBRefresh -VdbId $VdbId
    Write-Log ('Refresh job started: ' + $refreshJob.id + '. Waiting for completion ...')
    $finalRefreshJob = Wait-DCTJob -JobId $refreshJob.id
    Write-Log ('VDB refresh completed. Status: ' + $finalRefreshJob.status)
}
catch {
    Write-Log ('VDB refresh failed: ' + $_) -Level ERROR
    exit 1
}

# ------------------------------------------------------------
# 3. Stop VDB
# ------------------------------------------------------------
Write-Log ('Stopping VDB ' + $VdbId + ' ...')
try {
    $stopJob = Stop-VDB -VdbId $VdbId
    Write-Log ('Stop job started: ' + $stopJob.id + '. Waiting for completion ...')
    $finalStopJob = Wait-DCTJob -JobId $stopJob.id
    Write-Log ('VDB stopped. Status: ' + $finalStopJob.status)
}
catch {
    Write-Log ('VDB stop failed: ' + $_) -Level ERROR
    exit 1
}

# ------------------------------------------------------------
# 4. Start VDB
# ------------------------------------------------------------
Write-Log ('Starting VDB ' + $VdbId + ' ...')
try {
    $startJob = Start-VDB -VdbId $VdbId
    Write-Log ('Start job started: ' + $startJob.id + '. Waiting for completion ...')
    $finalStartJob = Wait-DCTJob -JobId $startJob.id
    Write-Log ('VDB started. Status: ' + $finalStartJob.status)
}
catch {
    Write-Log ('VDB start failed: ' + $_) -Level ERROR
    exit 1
}

# ------------------------------------------------------------
# 5. Execute Masking Job
# ------------------------------------------------------------
Write-Log ('Executing masking job ' + $MaskingJobId + ' ...')
try {
    $maskingJob = Invoke-MaskingJob -MaskingJobId $MaskingJobId -EngineId $EngineId
    Write-Log ('Masking job started: ' + $maskingJob.id + '. Waiting for completion (this may take a while) ...')
    $finalMaskingJob = Wait-DCTJob -JobId $maskingJob.id
    Write-Log ('Masking job completed. Status: ' + $finalMaskingJob.status)
}
catch {
    Write-Log ('Masking job failed: ' + $_) -Level ERROR
    exit 1
}

# ------------------------------------------------------------
# 6. Snapshot VDB
# ------------------------------------------------------------
Write-Log ('Taking snapshot of VDB ' + $VdbId + ' ...')
try {
    $snapshotJob = New-VDBSnapshot -VdbId $VdbId
    Write-Log ('Snapshot job started: ' + $snapshotJob.id + '. Waiting for completion ...')
    $finalSnapshotJob = Wait-DCTJob -JobId $snapshotJob.id
    Write-Log ('VDB snapshot completed. Status: ' + $finalSnapshotJob.status)
}
catch {
    Write-Log ('VDB snapshot failed: ' + $_) -Level ERROR
    exit 1
}

# ------------------------------------------------------------
# 7. Trigger Replication
# ------------------------------------------------------------
#Write-Log ('Triggering replication profile ' + $ReplicationProfileId + ' ...')
#try {
#    $replicationJob = Invoke-Replication -ReplicationProfileId $ReplicationProfileId
#    Write-Log ('Replication job started: ' + $replicationJob.id + '. Waiting for completion ...')
#    $finalReplicationJob = Wait-DCTJob -JobId $replicationJob.id
#    Write-Log ('Replication completed. Status: ' + $finalReplicationJob.status)
#}
#catch {
#    Write-Log ('Replication failed: ' + $_) -Level ERROR
#    exit 1
#}

Write-Log 'All operations completed successfully.'
