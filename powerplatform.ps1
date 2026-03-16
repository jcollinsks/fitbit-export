<#
.SYNOPSIS
    Collects Power Platform inventory data for Power BI dashboards.
.DESCRIPTION
    Pulls environments, apps, flows, connectors, connections, DLP policies,
    and usage analytics from Power Platform admin APIs. Outputs CSV files
    ready for Power BI import.

    Designed for large tenants (1000+ environments, 40K+ apps, 60K+ flows):
    - Parallel flow definition fetching via runspace pool (10 concurrent by default)
    - Automatic token refresh every 20 minutes with re-auth on 401
    - Throttle handling with exponential backoff on 429 responses
    - Streaming CSV writes per environment (low memory footprint)
    - Progress tracking with ETA
    - Error logging to Errors.csv (non-fatal — continues on per-environment failures)

    With -IncludeFlowDefinitions, fetches full definitions to extract triggers,
    actions, and endpoint URLs (SharePoint sites, SQL servers, HTTP endpoints).
    Uses a runspace pool for parallel HTTP calls — at 10 concurrent, 60K flows
    takes ~50 minutes instead of ~48 hours sequential.
.PARAMETER OutputPath
    Directory for CSV output files. Defaults to ./PowerPlatformExport.
.PARAMETER IncludeFlowDefinitions
    If set, fetches full definition for each flow to extract endpoint URLs.
    WARNING: One API call per flow — at 60K flows this adds many hours. Off by default.
    Without this flag, flows still get triggers/actions from definitionSummary but
    EndpointUrl columns will be blank.
.PARAMETER MaxFlowDefinitions
    Limit how many flow definitions to fetch (0 = unlimited). Use with -IncludeFlowDefinitions
    to sample a subset, e.g. -IncludeFlowDefinitions -MaxFlowDefinitions 500
.PARAMETER ThrottleLimit
    Max concurrent API calls for parallel flow definition fetching. Default 10.
    Increase for faster runs (20-25) or decrease if hitting rate limits (5).
.PARAMETER EnvironmentId
    Collect data for a single environment only (by EnvironmentId). Useful for
    testing or quick runs. Get the ID from the Power Platform admin center URL
    or from a previous Environments.csv export.
.PARAMETER UseDeviceCode
    Use device code authentication instead of interactive browser login.
    Default is interactive browser (works in most corporate environments).
.PARAMETER Resume
    Resume a previous interrupted run. Skips environments already processed
    (tracked in _checkpoint.txt) and appends to existing CSVs instead of overwriting.
.PARAMETER IncludePermissions
    If set, fetches sharing/permissions for apps and flows.
    WARNING: One API call per resource — at 100K resources this adds hours. Off by default.
.EXAMPLE
    .\powerplatform.ps1 -OutputPath C:\exports
    .\powerplatform.ps1 -IncludeFlowDefinitions -ThrottleLimit 20
    .\powerplatform.ps1 -EnvironmentId "abc-123-def" -IncludeFlowDefinitions
    .\powerplatform.ps1 -Resume
    .\powerplatform.ps1 -IncludePermissions
#>

param(
    [string]$OutputPath = "./PowerPlatformExport",
    [string]$EnvironmentId = "",
    [switch]$IncludePermissions,
    [switch]$IncludeFlowDefinitions,
    [switch]$UseDeviceCode,
    [switch]$Resume,
    [int]$MaxFlowDefinitions = 0,
    [int]$ThrottleLimit = 10
)

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH — with automatic token refresh
# ============================================================================

Write-Host "Connecting to Azure..." -ForegroundColor Cyan
$script:connectArgs = @{}
if ($UseDeviceCode) { $script:connectArgs['UseDeviceAuthentication'] = $true }
try {
    Connect-AzAccount @script:connectArgs | Out-Null
}
catch {
    Write-Host "  Auth failed ($($_.Exception.Message)), retrying..." -ForegroundColor DarkYellow
    Connect-AzAccount @script:connectArgs | Out-Null
}

$script:ppToken = $null
$script:ppTokenExpiry = [datetime]::MinValue
$script:flowToken = $null
$script:flowTokenExpiry = [datetime]::MinValue
$script:adminToken = $null
$script:adminTokenExpiry = [datetime]::MinValue

function Get-TokenString {
    param([securestring]$SecureToken)
    [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureToken))
}

function Get-PPToken {
    if ([datetime]::UtcNow -lt $script:ppTokenExpiry) { return $script:ppToken }
    Write-Host "  [Auth] Refreshing Power Platform token..." -ForegroundColor DarkGray
    try {
        $result = Get-AzAccessToken -ResourceUrl "https://service.powerapps.com/" -AsSecureString
    }
    catch {
        Write-Host "  [Auth] Session expired — re-authenticating..." -ForegroundColor Yellow
        Connect-AzAccount @script:connectArgs | Out-Null
        $result = Get-AzAccessToken -ResourceUrl "https://service.powerapps.com/" -AsSecureString
    }
    $script:ppToken = Get-TokenString $result.Token
    $script:ppTokenExpiry = [datetime]::UtcNow.AddMinutes(20)
    return $script:ppToken
}

function Get-FlowToken {
    if ([datetime]::UtcNow -lt $script:flowTokenExpiry) { return $script:flowToken }
    Write-Host "  [Auth] Refreshing Flow API token..." -ForegroundColor DarkGray
    try {
        $result = Get-AzAccessToken -ResourceUrl "https://service.flow.microsoft.com/" -AsSecureString
    }
    catch {
        Write-Host "  [Auth] Session expired — re-authenticating..." -ForegroundColor Yellow
        Connect-AzAccount @script:connectArgs | Out-Null
        $result = Get-AzAccessToken -ResourceUrl "https://service.flow.microsoft.com/" -AsSecureString
    }
    $script:flowToken = Get-TokenString $result.Token
    $script:flowTokenExpiry = [datetime]::UtcNow.AddMinutes(20)
    return $script:flowToken
}

function Get-AdminToken {
    if ([datetime]::UtcNow -lt $script:adminTokenExpiry) { return $script:adminToken }
    Write-Host "  [Auth] Refreshing Admin Center token..." -ForegroundColor DarkGray
    try {
        $result = Get-AzAccessToken -ResourceUrl "https://api.powerplatform.com/" -AsSecureString
    }
    catch {
        Write-Host "  [Auth] Session expired — re-authenticating..." -ForegroundColor Yellow
        Connect-AzAccount @script:connectArgs | Out-Null
        $result = Get-AzAccessToken -ResourceUrl "https://api.powerplatform.com/" -AsSecureString
    }
    $script:adminToken = Get-TokenString $result.Token
    $script:adminTokenExpiry = [datetime]::UtcNow.AddMinutes(20)
    return $script:adminToken
}

# Force-expire all cached tokens so the next API call gets a fresh one
function Reset-AllTokens {
    $script:ppTokenExpiry = [datetime]::MinValue
    $script:flowTokenExpiry = [datetime]::MinValue
    $script:adminTokenExpiry = [datetime]::MinValue
}

# ============================================================================
# API HELPERS — with throttle handling
# ============================================================================

function Invoke-PPApi {
    param(
        [string]$Uri,
        [string]$Token,
        [string]$Method = "GET",
        [int]$MaxRetries = 5,
        [scriptblock]$TokenRefresh = $null
    )
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $headers = @{ "Authorization" = "Bearer $Token"; "Accept" = "application/json" }
            return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers -TimeoutSec 60
        }
        catch {
            $status = $_.Exception.Response.StatusCode.value__
            if ($status -eq 401 -and $attempt -le 2) {
                # Token expired — refresh and retry (max 2 attempts, then give up)
                Write-Host "    [Auth] 401 Unauthorized — refreshing tokens (attempt $attempt/2)" -ForegroundColor Yellow
                Reset-AllTokens
                if ($TokenRefresh) { $Token = & $TokenRefresh }
            }
            elseif ($status -eq 401) {
                return $null  # Still 401 after refresh — no access to this resource
            }
            elseif ($status -eq 429 -and $attempt -lt $MaxRetries) {
                $retryAfter = 30 * [math]::Pow(2, $attempt - 1)  # 30s, 60s, 120s, 240s
                $retryHeader = $_.Exception.Response.Headers | Where-Object { $_.Key -eq "Retry-After" }
                if ($retryHeader) { $retryAfter = [int]$retryHeader.Value[0] }
                Write-Host "    [Throttled] 429 — waiting ${retryAfter}s (attempt $attempt/$MaxRetries)" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $retryAfter
            }
            elseif ($status -eq 403 -or $status -eq 404) {
                return $null  # Not found or forbidden is non-fatal
            }
            elseif ($attempt -lt $MaxRetries) {
                # Timeout or other transient error — wait and retry
                $wait = 5 * $attempt
                Write-Host "    [Retry] Error on attempt $attempt/$MaxRetries — waiting ${wait}s ($($_.Exception.Message))" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $wait
            }
            else {
                throw
            }
        }
    }
    throw "Max retries ($MaxRetries) exceeded for $Uri"
}

function Invoke-PPApiPaged {
    param(
        [string]$Uri,
        [string]$Token,
        [scriptblock]$TokenRefresh = { Get-PPToken },
        [int]$MaxPages = 500
    )
    $all = [System.Collections.Generic.List[object]]::new()
    $url = $Uri
    $page = 0
    $seenUrls = [System.Collections.Generic.HashSet[string]]::new()
    while ($url) {
        $page++
        if ($page -gt $MaxPages) {
            Write-Host "    [Paging] Hit $MaxPages page limit — stopping pagination ($($all.Count) items)" -ForegroundColor DarkYellow
            break
        }
        # Detect infinite loop — same URL seen twice
        if (-not $seenUrls.Add($url)) {
            Write-Host "    [Paging] Duplicate nextLink detected — breaking loop ($($all.Count) items)" -ForegroundColor DarkYellow
            break
        }
        $Token = & $TokenRefresh  # Refresh token if needed before each page
        $response = Invoke-PPApi -Uri $url -Token $Token -TokenRefresh $TokenRefresh
        if ($null -eq $response) { break }
        if ($response.value -and $response.value.Count -gt 0) {
            $all.AddRange([object[]]$response.value)
        }
        else {
            break  # Empty page — no more data regardless of nextLink
        }
        $url = if ($response.nextLink) { $response.nextLink }
               elseif ($response.'@odata.nextLink') { $response.'@odata.nextLink' }
               else { $null }
    }
    return $all
}

# ============================================================================
# CSV STREAMING HELPERS — append rows without holding everything in memory
# ============================================================================

function Initialize-Csv {
    param([string]$Path, [string[]]$Headers)
    $line = ($Headers | ForEach-Object { "`"$_`"" }) -join ","
    Set-Content -Path $Path -Value $line -Encoding UTF8
}

function Append-CsvRow {
    param([string]$Path, [PSCustomObject]$Row)
    $values = $Row.PSObject.Properties | ForEach-Object {
        $v = if ($null -eq $_.Value) { "" } else { "$($_.Value)" }
        $v = $v -replace "`"", "`"`""   # Escape quotes
        "`"$v`""
    }
    Add-Content -Path $Path -Value (($values) -join ",") -Encoding UTF8
}

function Append-CsvRows {
    param([string]$Path, [array]$Rows)
    foreach ($row in $Rows) { Append-CsvRow -Path $Path -Row $row }
}

# ============================================================================
# SETUP
# ============================================================================

New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
$bap = "https://api.bap.microsoft.com"
$flow = "https://api.flow.microsoft.com"
$pa = "https://api.powerapps.com"
$apiVer = "api-version=2016-11-01"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$startTime = Get-Date
$errors = [System.Collections.Generic.List[PSCustomObject]]::new()

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Power Platform Data Collection" -ForegroundColor Green
Write-Host " Output: $OutputPath" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# ============================================================================
# 1. ENVIRONMENTS
# ============================================================================

Write-Host "[1/7] Collecting environments..." -ForegroundColor Yellow
$token = Get-PPToken
$envUri = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?$apiVer&`$expand=properties.capacity,properties.addons"
$envs = Invoke-PPApiPaged -Uri $envUri -Token $token

$environments = $envs | ForEach-Object {
    $cap = $_.properties.capacity
    [PSCustomObject]@{
        EnvironmentId      = $_.name
        DisplayName        = $_.properties.displayName
        EnvironmentType    = $_.properties.environmentSku
        Region             = $_.properties.azureRegion
        State              = $_.properties.states.management.id
        IsDefault          = $_.properties.isDefault
        SecurityGroupId    = $_.properties.securityGroupId
        OrgUrl             = $_.properties.linkedEnvironmentMetadata.instanceUrl
        IsDataverseEnabled = [bool]$_.properties.linkedEnvironmentMetadata
        DatabaseUsedMb     = ($cap | Where-Object { $_.capacityType -eq "Database" }).actualConsumption
        FileUsedMb         = ($cap | Where-Object { $_.capacityType -eq "File" }).actualConsumption
        LogUsedMb          = ($cap | Where-Object { $_.capacityType -eq "Log" }).actualConsumption
        CreatedTime        = $_.properties.createdTime
        LastModifiedTime   = $_.properties.lastModifiedTime
        CollectedAt        = $timestamp
    }
}

# --- Filter to single environment if requested ---
if ($EnvironmentId -ne "") {
    $environments = @($environments | Where-Object { $_.EnvironmentId -eq $EnvironmentId })
    if ($environments.Count -eq 0) {
        Write-Host "  ERROR: Environment '$EnvironmentId' not found. Available:" -ForegroundColor Red
        $envs | ForEach-Object { Write-Host "    $($_.name)  ($($_.properties.displayName))" -ForegroundColor Gray }
        throw "Environment not found"
    }
    Write-Host "  Filtered to single environment: $($environments[0].DisplayName)" -ForegroundColor Cyan
}

$environments | Export-Csv "$OutputPath/Environments.csv" -NoTypeInformation
Write-Host "  Found $($environments.Count) environments" -ForegroundColor Gray

# ============================================================================
# 2-4. APPS, FLOWS, CONNECTORS — per-environment loop with streaming CSV
# ============================================================================

# --- CHECKPOINT: track completed environments for -Resume ---
$checkpointFile = "$OutputPath/_checkpoint.txt"
$completedEnvs = [System.Collections.Generic.HashSet[string]]::new()

if ($Resume -and (Test-Path $checkpointFile)) {
    $completedEnvs = [System.Collections.Generic.HashSet[string]]::new(
        [string[]]@(Get-Content $checkpointFile | Where-Object { $_.Trim() -ne "" })
    )
    Write-Host "  Resuming: $($completedEnvs.Count) environments already completed" -ForegroundColor Cyan
}

if ($Resume -and (Test-Path "$OutputPath/Apps.csv")) {
    # Append to existing CSVs
    Write-Host "  Appending to existing CSV files" -ForegroundColor Cyan
}
else {
    # Fresh run — initialize CSV files with headers
    Initialize-Csv "$OutputPath/Apps.csv" @("AppId","EnvironmentId","EnvironmentName","DisplayName","Description","AppType","OwnerObjectId","OwnerDisplayName","OwnerEmail","CreatedTime","LastModifiedTime","LastPublishedTime","AppVersion","Status","UsesPremiumApi","UsesCustomApi","SharedUsersCount","SharedGroupsCount","IsSolutionAware","SolutionId","BypassConsent","CollectedAt")
    Initialize-Csv "$OutputPath/AppConnectorRefs.csv" @("AppId","EnvironmentId","ConnectorId","DisplayName","DataSources","EndpointUrl")
    Initialize-Csv "$OutputPath/Flows.csv" @("FlowId","EnvironmentId","EnvironmentName","DisplayName","Description","State","CreatorObjectId","CreatorDisplayName","CreatedTime","LastModifiedTime","TriggerType","IsSolutionAware","SolutionId","IsManaged","SuspensionReason","CollectedAt")
    Initialize-Csv "$OutputPath/FlowTriggers.csv" @("FlowId","EnvironmentId","Position","Name","TriggerType","ConnectorId","OperationId","EndpointUrl")
    Initialize-Csv "$OutputPath/FlowActions.csv" @("FlowId","EnvironmentId","Position","Name","ActionType","ConnectorId","OperationId","EndpointUrl")
    Initialize-Csv "$OutputPath/FlowConnectionRefs.csv" @("FlowId","EnvironmentId","ConnectorId","ConnectionName","ConnectionUrl")
    Initialize-Csv "$OutputPath/Connectors.csv" @("ConnectorId","EnvironmentId","EnvironmentName","DisplayName","Description","Publisher","Tier","IsCustom","IconUri","CollectedAt")
    # Clear checkpoint for fresh run
    if (Test-Path $checkpointFile) { Remove-Item $checkpointFile }
}

$totalApps = 0; $totalFlows = 0; $totalConnectors = 0
$totalAppConnRefs = 0; $totalTriggers = 0; $totalActions = 0; $totalFlowConnRefs = 0
$envCount = $environments.Count
$envIndex = 0

Write-Host "[2-4/7] Collecting apps, flows, connectors per environment..." -ForegroundColor Yellow

foreach ($env in $environments) {
    $envIndex++
    $envId = $env.EnvironmentId

    # Skip already-completed environments on resume
    if ($completedEnvs.Contains($envId)) {
        continue
    }

    $elapsed = (Get-Date) - $startTime
    $remaining_count = $envCount - $envIndex - $completedEnvs.Count + ($completedEnvs.Count)
    $pct = [math]::Round(($envIndex / $envCount) * 100)
    $eta = if ($envIndex -gt 1) {
        $processed = $envIndex - $completedEnvs.Count
        if ($processed -gt 0) {
            $perEnv = $elapsed.TotalSeconds / $processed
            $remaining = [TimeSpan]::FromSeconds($perEnv * ($envCount - $envIndex))
            "{0:hh\:mm\:ss}" -f $remaining
        } else { "calculating..." }
    } else { "calculating..." }

    Write-Host "  [$envIndex/$envCount] $($env.DisplayName) ($pct% — ETA: $eta)" -ForegroundColor Gray

    # --- ACCESS CHECK — skip environments we can't read ---
    $token = Get-PPToken
    $accessCheck = Invoke-PPApi -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apps?$apiVer&`$top=1" -Token $token -TokenRefresh { Get-PPToken }
    if ($null -eq $accessCheck) {
        Write-Host "    SKIPPED — no access (401/403)" -ForegroundColor DarkYellow
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="AccessCheck"; Error="No access (401/403) — skipped entire environment"; Timestamp=(Get-Date) })
        continue
    }

    # --- Per-environment timeout (3 minutes max) ---
    $envStartTime = Get-Date
    $envTimeoutMin = 10

    # --- CONNECTORS ---
    try {
        Write-Host "    Connectors..." -ForegroundColor DarkGray -NoNewline
        $token = Get-PPToken
        $connectors = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apis?$apiVer&showApisWithToS=true" -Token $token
        if (-not $connectors) { $connectors = @() }
        foreach ($c in $connectors) {
            Append-CsvRow "$OutputPath/Connectors.csv" ([PSCustomObject]@{
                ConnectorId=($c.name -replace '.*/apis/', ''); EnvironmentId=$envId; EnvironmentName=$env.DisplayName
                DisplayName=$c.properties.displayName; Description=$c.properties.description
                Publisher=$c.properties.publisher
                Tier=$(if ($c.properties.tier) { $c.properties.tier } elseif ($c.properties.isCustomApi) { "Custom" } else { "Standard" })
                IsCustom=[bool]$c.properties.isCustomApi; IconUri=$c.properties.iconUri; CollectedAt=$timestamp
            })
            $totalConnectors++
        }
        Write-Host " $totalConnectors" -ForegroundColor DarkGray -NoNewline
    }
    catch {
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Connectors"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
        Write-Host " Warning (connectors): $($_.Exception.Message)" -ForegroundColor DarkYellow
    }

    # --- TIMEOUT CHECK ---
    if (((Get-Date) - $envStartTime).TotalMinutes -gt $envTimeoutMin) {
        Write-Host "    TIMEOUT — environment took >$envTimeoutMin min, skipping to next" -ForegroundColor Red
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Timeout"; Error="Environment processing exceeded ${envTimeoutMin}m limit"; Timestamp=(Get-Date) })
        continue
    }

    # --- APPS (after connections so we can resolve endpoint URLs) ---
    try {
        Write-Host " Apps..." -ForegroundColor DarkGray -NoNewline
        $token = Get-PPToken
        $apps = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apps?$apiVer" -Token $token
        foreach ($app in $apps) {
            # appType is at ROOT level in the API response, not under properties
            $appType = if ($app.appType) { $app.appType }
                       elseif ($app.properties.appType) { $app.properties.appType }
                       else { "CanvasApp" }
            $row = [PSCustomObject]@{
                AppId=$app.name; EnvironmentId=$envId; EnvironmentName=$env.DisplayName
                DisplayName=$app.properties.displayName; Description=$app.properties.description
                AppType=$appType; OwnerObjectId=$app.properties.owner.id
                OwnerDisplayName=$app.properties.owner.displayName; OwnerEmail=$app.properties.owner.email
                CreatedTime=$app.properties.createdTime; LastModifiedTime=$app.properties.lastModifiedTime
                LastPublishedTime=$app.properties.lastPublishedDate; AppVersion=$app.properties.appVersion
                Status=$app.properties.lifecycleId; UsesPremiumApi=$app.properties.usesPremiumApi
                UsesCustomApi=$app.properties.usesCustomApi; SharedUsersCount=$app.properties.sharedUsersCount
                SharedGroupsCount=$app.properties.sharedGroupsCount; IsSolutionAware=$app.properties.isSolutionAware
                SolutionId=$app.properties.solutionId; BypassConsent=$app.properties.bypassConsent
                CollectedAt=$timestamp
            }
            Append-CsvRow "$OutputPath/Apps.csv" $row
            $totalApps++

            # Extract connection references
            if ($app.properties.connectionReferences) {
                foreach ($ref in $app.properties.connectionReferences.PSObject.Properties) {
                    $connId = $ref.Value.id -replace '.*/apis/', ''
                    Append-CsvRow "$OutputPath/AppConnectorRefs.csv" ([PSCustomObject]@{
                        AppId=$app.name; EnvironmentId=$envId; ConnectorId=$connId
                        DisplayName=$ref.Value.displayName; DataSources=($ref.Value.dataSources -join "; ")
                        EndpointUrl=""
                    })
                    $totalAppConnRefs++
                }
            }
        }
    }
    catch {
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Apps"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
        Write-Host "    Warning (apps): $($_.Exception.Message)" -ForegroundColor DarkYellow
    }

    # --- TIMEOUT CHECK ---
    if (((Get-Date) - $envStartTime).TotalMinutes -gt $envTimeoutMin) {
        Write-Host ""
        Write-Host "    TIMEOUT — environment took >$envTimeoutMin min, skipping to next" -ForegroundColor Red
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Timeout"; Error="Environment processing exceeded ${envTimeoutMin}m limit"; Timestamp=(Get-Date) })
        continue
    }

    # --- FLOWS ---
    try {
        Write-Host " Flows..." -ForegroundColor DarkGray
        $flowsUri = "$flow/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/v2/flows?$apiVer"
        # Try flow-scoped token first, fall back to PowerApps token
        $flows = $null
        $flowTokenRefresh = { Get-FlowToken }
        try {
            $fToken = Get-FlowToken
            $flows = Invoke-PPApiPaged -Uri $flowsUri -Token $fToken -TokenRefresh $flowTokenRefresh
        }
        catch {
            Write-Host "    Flow token failed ($($_.Exception.Message)), trying PowerApps token..." -ForegroundColor DarkGray
            $flowTokenRefresh = { Get-PPToken }
            $fToken = Get-PPToken
            $flows = Invoke-PPApiPaged -Uri $flowsUri -Token $fToken -TokenRefresh $flowTokenRefresh
        }
        if ($flows -and $flows.Count -gt 0) {
            $fetchDefs = $IncludeFlowDefinitions
            $defLimit = if ($MaxFlowDefinitions -gt 0) { $MaxFlowDefinitions } else { $flows.Count }
            $defMsg = if ($fetchDefs) { "fetching definitions (limit: $defLimit, $ThrottleLimit concurrent)..." } else { "using list data (use -IncludeFlowDefinitions for endpoint URLs)..." }
            Write-Host "    Found $($flows.Count) flows — $defMsg" -ForegroundColor DarkGray
        }

        # --- PARALLEL FETCH flow definitions using runspace pool ---
        $flowDefCache = @{}
        if ($fetchDefs -and $flows -and $flows.Count -gt 0) {
            $flowsToFetch = @(if ($defLimit -lt $flows.Count) { $flows | Select-Object -First $defLimit } else { $flows })
            $fetchCount = $flowsToFetch.Count

            Write-Host "      Parallel-fetching $fetchCount definitions ($ThrottleLimit concurrent)..." -ForegroundColor Cyan

            $pool = [runspacefactory]::CreateRunspacePool(1, $ThrottleLimit)
            $pool.Open()

            $fetchScript = {
                param([string]$Uri, [string]$BearerToken)
                for ($r = 1; $r -le 3; $r++) {
                    try {
                        $h = @{ "Authorization" = "Bearer $BearerToken"; "Accept" = "application/json" }
                        return (Invoke-RestMethod -Uri $Uri -Method GET -Headers $h -TimeoutSec 30)
                    }
                    catch {
                        $sc = 0
                        try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                        if ($sc -eq 429) { Start-Sleep -Seconds ([math]::Min(10 * $r, 30)); continue }
                        if ($sc -eq 403 -or $sc -eq 404 -or $sc -eq 401) { return $null }
                        if ($r -eq 3) { return $null }
                    }
                }
                return $null
            }

            # Process in batches of 50 — refresh token between batches, frequent progress
            $batchSize = [math]::Min(50, $fetchCount)
            $batchStart = 0
            $batchTimeoutMs = 120000  # 2 min max per batch — kill hung jobs

            while ($batchStart -lt $fetchCount) {
                $batchEnd = [math]::Min($batchStart + $batchSize, $fetchCount)
                $batchToken = & $flowTokenRefresh  # Fresh token each batch

                $pending = [System.Collections.Generic.List[hashtable]]::new()
                for ($bi = $batchStart; $bi -lt $batchEnd; $bi++) {
                    $ff = $flowsToFetch[$bi]
                    $fetchUri = "$flow/providers/Microsoft.ProcessSimple/environments/$envId/flows/$($ff.name)?$apiVer"

                    $ps = [powershell]::Create()
                    $ps.RunspacePool = $pool
                    [void]$ps.AddScript($fetchScript).AddArgument($fetchUri).AddArgument($batchToken)

                    $pending.Add(@{ PS = $ps; Handle = $ps.BeginInvoke(); FlowId = $ff.name })
                }

                foreach ($pj in $pending) {
                    try {
                        # Wait with timeout — don't block forever on hung requests
                        if ($pj.Handle.AsyncWaitHandle.WaitOne($batchTimeoutMs)) {
                            $res = $pj.PS.EndInvoke($pj.Handle)
                            if ($res) { $flowDefCache[$pj.FlowId] = $res }
                        }
                        else {
                            # Job hung — kill it and move on
                            $pj.PS.Stop()
                        }
                    }
                    catch {}
                    $pj.PS.Dispose()
                }

                $batchStart = $batchEnd
                $pct = [math]::Round(($batchStart / $fetchCount) * 100)
                Write-Host "      Definitions: $batchStart / $fetchCount ($pct%) — cached $($flowDefCache.Count)" -ForegroundColor DarkGray
            }

            $pool.Close(); $pool.Dispose()
            Write-Host "      Parallel fetch complete: $($flowDefCache.Count) / $fetchCount definitions" -ForegroundColor Cyan
        }

        $flowIndex = 0
        foreach ($f in $flows) {
            $flowIndex++
            if ($flowIndex % 100 -eq 0) {
                $flowPct = [math]::Round(($flowIndex / $flows.Count) * 100)
                Write-Host "      Flows: $flowIndex / $($flows.Count) ($flowPct%)" -ForegroundColor DarkGray
            }

            # Use data from the V2 list response (always available, no extra API call)
            $flowDefinition = $null
            $defSummary = $f.properties.definitionSummary
            $creatorId = if ($f.properties.creator) { $f.properties.creator.objectId } else { "" }
            $creatorName = if ($f.properties.creator) { $f.properties.creator.displayName } else { "" }
            $connRefs = $f.properties.connectionReferences

            # Use pre-fetched definition from parallel cache
            if ($fetchDefs -and $flowDefCache.ContainsKey($f.name)) {
                $flowDetail = $flowDefCache[$f.name]
                if ($flowDetail) {
                    $flowDefinition = $flowDetail.properties.definition
                    if ($flowDetail.properties.definitionSummary) { $defSummary = $flowDetail.properties.definitionSummary }
                    if ($flowDetail.properties.creator) {
                        $creatorId = $flowDetail.properties.creator.objectId
                        $creatorName = $flowDetail.properties.creator.displayName
                    }
                    if ($flowDetail.properties.connectionReferences) { $connRefs = $flowDetail.properties.connectionReferences }
                }
            }

            # --- Helper: extract endpoint URL from step input parameters ---
            # This is where the actual URLs live (SharePoint sites, SQL servers, HTTP endpoints)
            function Get-StepEndpointUrl {
                param($Inputs)
                if (-not $Inputs) { return "" }
                # Inputs can be a scalar (Compose actions) — only process objects
                if ($Inputs -is [string] -or $Inputs -is [int] -or $Inputs -is [bool] -or
                    $null -eq $Inputs.PSObject) { return "" }

                # --- HTTP connector: uri/url directly on inputs (shared_sendhttp, httpwebhook) ---
                # Check this FIRST — HTTP actions have uri at the root level alongside method/headers.
                # The value may be a string or an expression object; extract string if possible.
                foreach ($uriKey in @('uri','url')) {
                    if ($Inputs.PSObject.Properties.Name -contains $uriKey) {
                        $raw = $Inputs.$uriKey
                        if ($raw -is [string] -and $raw -ne '') { return $raw }
                        # Expression object — try to get the literal value
                        if ($raw -and $null -ne $raw.PSObject) {
                            $s = "$raw"
                            if ($s -and $s -ne '' -and $s -notmatch '^System\.') { return $s }
                        }
                    }
                }

                $params = $null
                if ($Inputs.PSObject.Properties.Name -contains 'parameters') { $params = $Inputs.parameters }
                if (-not $params) { return "" }
                # params could also be a non-object (expression string)
                if ($params -is [string] -or $null -eq $params.PSObject) { return "" }

                # --- HTTP with Azure AD (Entra): uri inside parameters ---
                foreach ($uriKey in @('uri','url')) {
                    if ($params.PSObject.Properties.Name -contains $uriKey) {
                        $raw = $params.$uriKey
                        if ($raw -is [string] -and $raw -ne '') { return $raw }
                        if ($raw -and $null -ne $raw.PSObject) {
                            $s = "$raw"
                            if ($s -and $s -ne '' -and $s -notmatch '^System\.') { return $s }
                        }
                    }
                }

                # SharePoint: dataset = site URL
                if ($params.PSObject.Properties.Name -contains 'dataset') {
                    $v = "$($params.dataset)"
                    if ($v -and $v -ne '') { return $v }
                }
                # SQL: server/database
                if ($params.PSObject.Properties.Name -contains 'server') {
                    $v = "$($params.server)"
                    if ($params.PSObject.Properties.Name -contains 'database') { $v = "$v/$($params.database)" }
                    if ($v -and $v -ne '' -and $v -ne '/') { return $v }
                }
                # Generic URL fields
                foreach ($key in @('siteUrl','token:siteUrl','serviceUrl','baseUrl','endpoint','hostname','hostName')) {
                    if ($params.PSObject.Properties.Name -contains $key) {
                        $v = "$($params.$key)"
                        if ($v -and $v -ne '') { return $v }
                    }
                }
                # Dataverse: entity name as "resource"
                if ($params.PSObject.Properties.Name -contains 'entityName') {
                    return "dataverse:$($params.entityName)"
                }
                if ($params.PSObject.Properties.Name -contains 'subscriptionRequest/entityname') {
                    return "dataverse:$($params.'subscriptionRequest/entityname')"
                }
                return ""
            }

            # --- Helper: extract host info (connectorId, operationId, connectionName) from step inputs ---
            function Get-StepHostInfo {
                param($Inputs)
                $info = @{ ConnectorId = ""; OperationId = ""; ConnectionName = "" }
                if (-not $Inputs) { return $info }
                # Inputs can be a scalar (Compose actions) — only process objects
                if ($Inputs -is [string] -or $Inputs -is [int] -or $Inputs -is [bool] -or
                    $null -eq $Inputs.PSObject) { return $info }
                if (-not ($Inputs.PSObject.Properties.Name -contains 'host')) { return $info }
                $host_ = $Inputs.host
                if (-not $host_ -or $null -eq $host_.PSObject) { return $info }
                if ($host_.PSObject.Properties.Name -contains 'apiId') {
                    $info.ConnectorId = "$($host_.apiId)" -replace '.*/apis/', ''
                }
                if ($host_.PSObject.Properties.Name -contains 'operationId') {
                    $info.OperationId = "$($host_.operationId)"
                }
                if ($host_.PSObject.Properties.Name -contains 'connectionName') {
                    $info.ConnectionName = "$($host_.connectionName)"
                }
                return $info
            }

            # --- Helper: recursively write actions from definition to CSV ---
            # Writes directly to CSV instead of returning a list (avoids PowerShell pipeline
            # unrolling which turns List<T> into $null/single-object on return)
            function Write-FlattenedActions {
                param($ActionsObj, [string]$FlowId, [string]$EnvId, [string]$OutPath, [ref]$Pos, [ref]$Count)
                if (-not $ActionsObj -or $null -eq $ActionsObj.PSObject) { return }
                foreach ($prop in $ActionsObj.PSObject.Properties) {
                    $stepName = $prop.Name
                    $step = $prop.Value
                    if (-not $step -or $null -eq $step.PSObject) { continue }
                    $stepType = if ($step.PSObject.Properties.Name -contains 'type') { $step.type } else { "Unknown" }
                    $inputs = if ($step.PSObject.Properties.Name -contains 'inputs') { $step.inputs } else { $null }
                    $hostInfo = Get-StepHostInfo $inputs
                    $epUrl = Get-StepEndpointUrl $inputs

                    Append-CsvRow "$OutPath/FlowActions.csv" ([PSCustomObject]@{
                        FlowId=$FlowId; EnvironmentId=$EnvId; Position=$Pos.Value; Name=$stepName
                        ActionType=$stepType; ConnectorId=$hostInfo.ConnectorId
                        OperationId=$hostInfo.OperationId; EndpointUrl=$epUrl
                    })
                    $Pos.Value++
                    $Count.Value++

                    # Recurse into nested actions: Scope, ForEach, Until have .actions
                    if ($step.PSObject.Properties.Name -contains 'actions' -and $step.actions) {
                        Write-FlattenedActions $step.actions $FlowId $EnvId $OutPath $Pos $Count
                    }
                    # Condition: .else.actions
                    if ($step.PSObject.Properties.Name -contains 'else' -and $step.else -and
                        $step.else.PSObject.Properties.Name -contains 'actions' -and $step.else.actions) {
                        Write-FlattenedActions $step.else.actions $FlowId $EnvId $OutPath $Pos $Count
                    }
                    # Switch: .cases.*.actions and .default.actions
                    if ($step.PSObject.Properties.Name -contains 'cases' -and $step.cases) {
                        foreach ($caseProp in $step.cases.PSObject.Properties) {
                            if ($caseProp.Value.PSObject.Properties.Name -contains 'actions' -and $caseProp.Value.actions) {
                                Write-FlattenedActions $caseProp.Value.actions $FlowId $EnvId $OutPath $Pos $Count
                            }
                        }
                    }
                    if ($step.PSObject.Properties.Name -contains 'default' -and $step.default -and
                        $step.default.PSObject.Properties.Name -contains 'actions' -and $step.default.actions) {
                        Write-FlattenedActions $step.default.actions $FlowId $EnvId $OutPath $Pos $Count
                    }
                }
            }

            # --- Write FlowConnectionRefs from connectionReferences ---
            if ($connRefs) {
                foreach ($ref in $connRefs.PSObject.Properties) {
                    $crConnId = if ($ref.Value.PSObject.Properties.Name -contains 'id') { $ref.Value.id -replace '.*/apis/', '' }
                                elseif ($ref.Value.PSObject.Properties.Name -contains 'api' -and $ref.Value.api.name) { $ref.Value.api.name }
                                else { $ref.Name }
                    $crConnName = if ($ref.Value.PSObject.Properties.Name -contains 'connectionName') { $ref.Value.connectionName }
                                  else { "" }

                    Append-CsvRow "$OutputPath/FlowConnectionRefs.csv" ([PSCustomObject]@{
                        FlowId=$f.name; EnvironmentId=$envId; ConnectorId=$crConnId; ConnectionName=$crConnName; ConnectionUrl=""
                    })
                    $totalFlowConnRefs++
                }
            }

            # --- Parse triggers and actions ---
            $triggerType = "Unknown"
            $usedFullDef = $false

            try {
                # PREFERRED: Parse from full definition (has input parameters with actual URLs)
                if ($flowDefinition -and $null -ne $flowDefinition.PSObject -and
                    $flowDefinition.PSObject.Properties.Name -contains 'triggers' -and $flowDefinition.triggers) {
                    $usedFullDef = $true
                    $pos = 0
                    foreach ($prop in $flowDefinition.triggers.PSObject.Properties) {
                        $stepName = $prop.Name
                        $trig = $prop.Value
                        if (-not $trig -or $null -eq $trig.PSObject) { continue }
                        $tType = if ($trig.PSObject.Properties.Name -contains 'type') { $trig.type } else { "Unknown" }
                        $inputs = if ($trig.PSObject.Properties.Name -contains 'inputs') { $trig.inputs } else { $null }
                        $hostInfo = Get-StepHostInfo $inputs
                        $epUrl = Get-StepEndpointUrl $inputs
                        if ($pos -eq 0) { $triggerType = $tType }
                        Append-CsvRow "$OutputPath/FlowTriggers.csv" ([PSCustomObject]@{
                            FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=$stepName
                            TriggerType=$tType; ConnectorId=$hostInfo.ConnectorId
                            OperationId=$hostInfo.OperationId; EndpointUrl=$epUrl
                        })
                        $pos++; $totalTriggers++
                    }

                    if ($flowDefinition.PSObject.Properties.Name -contains 'actions' -and $flowDefinition.actions) {
                        $actionPos = [ref]0
                        $actionCount = [ref]0
                        Write-FlattenedActions $flowDefinition.actions $f.name $envId $OutputPath $actionPos $actionCount
                        $totalActions += $actionCount.Value
                    }
                }

                # FALLBACK: Parse from definitionSummary (no URLs, but we get connector IDs)
                if (-not $usedFullDef -and $defSummary) {
                    if ($defSummary.triggers) {
                        $pos = 0
                        foreach ($t in $defSummary.triggers) {
                            $tConnId = if ($t.api -and $t.api.id) { $t.api.id -replace '.*/apis/', '' } else { "" }
                            if ($pos -eq 0) { $triggerType = $t.type }
                            Append-CsvRow "$OutputPath/FlowTriggers.csv" ([PSCustomObject]@{
                                FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                                TriggerType=$t.type; ConnectorId=$tConnId; OperationId=$t.swaggerOperationId; EndpointUrl=""
                            })
                            $pos++; $totalTriggers++
                        }
                    }
                    if ($defSummary.actions) {
                        $pos = 0
                        foreach ($a in $defSummary.actions) {
                            $aConnId = if ($a.api -and $a.api.id) { $a.api.id -replace '.*/apis/', '' } else { "" }
                            Append-CsvRow "$OutputPath/FlowActions.csv" ([PSCustomObject]@{
                                FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                                ActionType=$a.type; ConnectorId=$aConnId; OperationId=$a.swaggerOperationId; EndpointUrl=""
                            })
                            $pos++; $totalActions++
                        }
                    }
                }
            }
            catch {
                Write-Host "      Warning: Failed to parse definition for flow $($f.name): $($_.Exception.Message)" -ForegroundColor DarkYellow
            }

            Append-CsvRow "$OutputPath/Flows.csv" ([PSCustomObject]@{
                FlowId=$f.name; EnvironmentId=$envId; EnvironmentName=$env.DisplayName
                DisplayName=$f.properties.displayName; Description=$f.properties.description
                State=$f.properties.state; CreatorObjectId=$creatorId
                CreatorDisplayName=$creatorName; CreatedTime=$f.properties.createdTime
                LastModifiedTime=$f.properties.lastModifiedTime; TriggerType=$triggerType
                IsSolutionAware=$f.properties.isSolutionAware; SolutionId=$f.properties.solutionId
                IsManaged=$f.properties.isManaged; SuspensionReason=$f.properties.flowSuspensionReason
                CollectedAt=$timestamp
            })
            $totalFlows++
        }
    }
    catch {
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Flows"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
        Write-Host "    Warning (flows): $($_.Exception.Message)" -ForegroundColor DarkYellow
    }

    # Mark environment as completed in checkpoint
    Add-Content -Path $checkpointFile -Value $envId -Encoding UTF8
    [void]$completedEnvs.Add($envId)
}

Write-Host "  Totals: $totalApps apps, $totalFlows flows, $totalConnectors connectors" -ForegroundColor Gray
Write-Host "  Totals: $totalAppConnRefs app-connector refs, $totalFlowConnRefs flow-connector refs, $totalTriggers triggers, $totalActions actions" -ForegroundColor Gray

# ============================================================================
# 5. DLP POLICIES (tenant-level, not per-environment)
# ============================================================================

Write-Host "[5/7] Collecting DLP policies..." -ForegroundColor Yellow
$token = Get-PPToken
$dlpUri = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies?$apiVer"
$dlps = Invoke-PPApiPaged -Uri $dlpUri -Token $token

$allDlpPolicies = @()
$allDlpConnectorRules = @()

foreach ($d in $dlps) {
    $allDlpPolicies += [PSCustomObject]@{
        PolicyId=$d.name; DisplayName=$d.properties.displayName; Description=$d.properties.description
        IsEnabled=(-not $d.properties.isDisabled); PolicyType=$d.properties.type
        EnvironmentScope=$d.properties.environmentFilterType
        CreatedTime=$d.properties.createdTime; LastModifiedTime=$d.properties.lastModifiedTime; CollectedAt=$timestamp
    }

    if ($d.properties.connectorGroups) {
        foreach ($group in $d.properties.connectorGroups) {
            foreach ($conn in $group.connectors) {
                $allDlpConnectorRules += [PSCustomObject]@{
                    PolicyId=$d.name; PolicyName=$d.properties.displayName
                    ConnectorId=($conn.id -replace '.*/apis/', ''); ConnectorName=$conn.name
                    Classification=$group.classification
                }
            }
        }
    }
    elseif ($d.properties.definition -and $d.properties.definition.apiGroups) {
        foreach ($group in $d.properties.definition.apiGroups.PSObject.Properties) {
            $cls = switch ($group.Name) { "hpiGroup" {"Business"} "lbiGroup" {"NonBusiness"} "blockedGroup" {"Blocked"} default {$group.Name} }
            foreach ($conn in $group.Value.apis) {
                $allDlpConnectorRules += [PSCustomObject]@{
                    PolicyId=$d.name; PolicyName=$d.properties.displayName
                    ConnectorId=($conn.id -replace '.*/apis/', ''); ConnectorName=$conn.name; Classification=$cls
                }
            }
        }
    }
}

$allDlpPolicies | Export-Csv "$OutputPath/DlpPolicies.csv" -NoTypeInformation
$allDlpConnectorRules | Export-Csv "$OutputPath/DlpConnectorRules.csv" -NoTypeInformation
Write-Host "  Found $($allDlpPolicies.Count) policies, $($allDlpConnectorRules.Count) connector rules" -ForegroundColor Gray

# ============================================================================
# 6. USAGE ANALYTICS
# ============================================================================

Write-Host "[6/7] Collecting usage analytics..." -ForegroundColor Yellow
$usageCollected = $false
# Try BAP analytics endpoint with both token types
foreach ($tokenName in @("PP", "Admin")) {
    if ($usageCollected) { break }
    try {
        $uToken = if ($tokenName -eq "PP") { Get-PPToken } else { Get-AdminToken }
        $usageUri = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?$apiVer"
        $usage = Invoke-PPApiPaged -Uri $usageUri -Token $uToken
        if ($usage -and $usage.Count -gt 0) {
            $allUsage = $usage | ForEach-Object {
                [PSCustomObject]@{
                    ResourceType=$_.resourceType; EnvironmentId=$_.environmentId; Date=$_.date
                    UniqueUsers=$_.uniqueUsers; TotalSessions=$_.totalSessions; TotalActions=$_.totalActions; CollectedAt=$timestamp
                }
            }
            $allUsage | Export-Csv "$OutputPath/UsageAnalytics.csv" -NoTypeInformation
            Write-Host "  Found $($allUsage.Count) usage records (via $tokenName token)" -ForegroundColor Gray
            $usageCollected = $true
        }
    }
    catch {
        Write-Host "  Note: Usage analytics with $tokenName token failed: $($_.Exception.Message)" -ForegroundColor DarkGray
    }
}
if (-not $usageCollected) {
    # Fallback: build basic usage summary from already-collected data
    Write-Host "  Usage analytics API unavailable — building summary from collected data" -ForegroundColor DarkYellow
    $usageSummary = @()
    foreach ($env in $environments) {
        $envApps = (Import-Csv "$OutputPath/Apps.csv" | Where-Object { $_.EnvironmentId -eq $env.EnvironmentId }).Count
        $envFlows = (Import-Csv "$OutputPath/Flows.csv" | Where-Object { $_.EnvironmentId -eq $env.EnvironmentId }).Count
        if ($envApps -gt 0) {
            $usageSummary += [PSCustomObject]@{
                ResourceType="PowerApp"; EnvironmentId=$env.EnvironmentId; Date=$timestamp
                UniqueUsers=0; TotalSessions=0; TotalActions=$envApps; CollectedAt=$timestamp
            }
        }
        if ($envFlows -gt 0) {
            $usageSummary += [PSCustomObject]@{
                ResourceType="Flow"; EnvironmentId=$env.EnvironmentId; Date=$timestamp
                UniqueUsers=0; TotalSessions=0; TotalActions=$envFlows; CollectedAt=$timestamp
            }
        }
    }
    $usageSummary | Export-Csv "$OutputPath/UsageAnalytics.csv" -NoTypeInformation
    Write-Host "  Built $($usageSummary.Count) summary records from inventory data" -ForegroundColor Gray
}

# ============================================================================
# 7. PERMISSIONS (optional — very slow at scale)
# ============================================================================

if ($IncludePermissions) {
    Write-Host "[7/7] Collecting permissions (this will take a while at scale)..." -ForegroundColor Yellow
    Initialize-Csv "$OutputPath/AppPermissions.csv" @("AppId","AppName","EnvironmentId","PrincipalId","PrincipalType","PrincipalDisplay","PrincipalEmail","RoleName")
    Initialize-Csv "$OutputPath/FlowPermissions.csv" @("FlowId","FlowName","EnvironmentId","PrincipalId","PrincipalType","PrincipalDisplay","PrincipalEmail","RoleName")

    # Re-read apps and flows from CSVs (streaming — we didn't keep them in memory)
    $appsCsv = Import-Csv "$OutputPath/Apps.csv"
    $i = 0; $totalAppPerms = 0
    foreach ($app in $appsCsv) {
        $i++
        if ($i % 100 -eq 0) {
            Write-Host "    App permissions: $i / $($appsCsv.Count) ($totalAppPerms perms)" -ForegroundColor DarkGray
        }
        try {
            $token = Get-PPToken
            $perms = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$($app.EnvironmentId)/apps/$($app.AppId)/permissions?$apiVer" -Token $token
            foreach ($p in $perms) {
                Append-CsvRow "$OutputPath/AppPermissions.csv" ([PSCustomObject]@{
                    AppId=$app.AppId; AppName=$app.DisplayName; EnvironmentId=$app.EnvironmentId
                    PrincipalId=$p.properties.principal.id; PrincipalType=$p.properties.principal.type
                    PrincipalDisplay=$p.properties.principal.displayName; PrincipalEmail=$p.properties.principal.email
                    RoleName=$p.properties.roleName
                })
                $totalAppPerms++
            }
        }
        catch { }
    }

    $flowsCsv = Import-Csv "$OutputPath/Flows.csv"
    $i = 0; $totalFlowPerms = 0
    foreach ($f in $flowsCsv) {
        $i++
        if ($i % 100 -eq 0) {
            Write-Host "    Flow permissions: $i / $($flowsCsv.Count) ($totalFlowPerms perms)" -ForegroundColor DarkGray
        }
        try {
            $fToken = Get-FlowToken
            $perms = Invoke-PPApiPaged -Uri "$flow/providers/Microsoft.ProcessSimple/scopes/admin/environments/$($f.EnvironmentId)/flows/$($f.FlowId)/permissions?$apiVer" -Token $fToken -TokenRefresh { Get-FlowToken }
            foreach ($p in $perms) {
                Append-CsvRow "$OutputPath/FlowPermissions.csv" ([PSCustomObject]@{
                    FlowId=$f.FlowId; FlowName=$f.DisplayName; EnvironmentId=$f.EnvironmentId
                    PrincipalId=$p.properties.principal.id; PrincipalType=$p.properties.principal.type
                    PrincipalDisplay=$p.properties.principal.displayName; PrincipalEmail=$p.properties.principal.email
                    RoleName=$p.properties.roleName
                })
                $totalFlowPerms++
            }
        }
        catch { }
    }

    Write-Host "  Found $totalAppPerms app permissions, $totalFlowPerms flow permissions" -ForegroundColor Gray
}
else {
    Write-Host "[7/7] Skipping permissions (use -IncludePermissions to collect)" -ForegroundColor DarkGray
}

# ============================================================================
# ERROR LOG & SUMMARY
# ============================================================================

if ($errors.Count -gt 0) {
    $errors | Export-Csv "$OutputPath/Errors.csv" -NoTypeInformation
    Write-Host ""
    Write-Host "  $($errors.Count) errors logged to Errors.csv" -ForegroundColor DarkYellow
}

$totalElapsed = (Get-Date) - $startTime

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Collection complete!" -ForegroundColor Green
Write-Host " Duration: $("{0:hh\:mm\:ss}" -f $totalElapsed)" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Output files in: $OutputPath" -ForegroundColor Cyan
Get-ChildItem "$OutputPath/*.csv" | ForEach-Object {
    $size = "{0:N1} MB" -f ($_.Length / 1MB)
    Write-Host "  $($_.Name) — $size" -ForegroundColor Gray
}
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open Power BI Desktop" -ForegroundColor Gray
Write-Host "  2. Get Data > Folder > select $OutputPath" -ForegroundColor Gray
Write-Host "  3. Or Get Data > Text/CSV for individual files" -ForegroundColor Gray
Write-Host "  4. Create relationships:" -ForegroundColor Gray
Write-Host "     - Apps.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host "     - Flows.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host "     - AppConnectorRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.FlowId -> Flows.FlowId" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - DlpConnectorRules.PolicyId -> DlpPolicies.PolicyId" -ForegroundColor Gray
Write-Host "     - UsageAnalytics.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host ""
