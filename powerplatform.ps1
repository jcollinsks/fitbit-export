<#
.SYNOPSIS
    Collects Power Platform inventory data for Power BI dashboards.
.DESCRIPTION
    Pulls environments, apps, flows, connectors, connections, DLP policies,
    usage analytics, and Copilot Studio agents from Power Platform admin APIs
    and Dataverse OData. Outputs CSV files ready for Power BI import.

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

$script:bapToken = $null
$script:bapTokenExpiry = [datetime]::MinValue
function Get-BapToken {
    if ([datetime]::UtcNow -lt $script:bapTokenExpiry) { return $script:bapToken }
    Write-Host "  [Auth] Refreshing BAP token..." -ForegroundColor DarkGray
    try {
        $result = Get-AzAccessToken -ResourceUrl "https://api.bap.microsoft.com/" -AsSecureString
    }
    catch {
        Write-Host "  [Auth] Session expired — re-authenticating..." -ForegroundColor Yellow
        Connect-AzAccount @script:connectArgs | Out-Null
        $result = Get-AzAccessToken -ResourceUrl "https://api.bap.microsoft.com/" -AsSecureString
    }
    $script:bapToken = Get-TokenString $result.Token
    $script:bapTokenExpiry = [datetime]::UtcNow.AddMinutes(20)
    return $script:bapToken
}

# Force-expire all cached tokens so the next API call gets a fresh one
function Reset-AllTokens {
    $script:ppTokenExpiry = [datetime]::MinValue
    $script:flowTokenExpiry = [datetime]::MinValue
    $script:adminTokenExpiry = [datetime]::MinValue
}

# --- Dataverse OData token (per-environment, cached by OrgUrl) ---
$script:dvTokens = @{}
function Get-DataverseToken {
    param([string]$OrgUrl)
    $resource = $OrgUrl.TrimEnd('/')
    $cached = $script:dvTokens[$resource]
    if ($cached -and [datetime]::UtcNow -lt $cached.Expiry) { return $cached.Token }
    try {
        $result = Get-AzAccessToken -ResourceUrl $resource -AsSecureString
    }
    catch {
        Write-Host " [Auth] Dataverse token failed, re-authenticating..." -ForegroundColor DarkGray
        try {
            Connect-AzAccount @script:connectArgs | Out-Null
            $result = Get-AzAccessToken -ResourceUrl $resource -AsSecureString
        }
        catch {
            Write-Host " [Auth] Dataverse token failed for $resource : $($_.Exception.Message)" -ForegroundColor DarkYellow
            return $null
        }
    }
    $token = Get-TokenString $result.Token
    $script:dvTokens[$resource] = @{ Token = $token; Expiry = [datetime]::UtcNow.AddMinutes(20) }
    return $token
}

function Invoke-DataverseOData {
    param([string]$OrgUrl, [string]$Query, [string]$Token, [int]$MaxPages = 100)
    $all = [System.Collections.Generic.List[object]]::new()
    $baseUri = "$($OrgUrl.TrimEnd('/'))/api/data/v9.2/"
    $uri = "$baseUri$Query"
    $page = 0
    while ($uri) {
        $page++
        if ($page -gt $MaxPages) { break }
        try {
            $response = Invoke-RestMethod -Uri $uri -Method Get -Headers @{
                "Authorization" = "Bearer $Token"
                "Accept" = "application/json"
                "OData-MaxVersion" = "4.0"
                "OData-Version" = "4.0"
                "Prefer" = 'odata.include-annotations="OData.Community.Display.V1.FormattedValue",odata.maxpagesize=5000'
            } -TimeoutSec 60
        }
        catch {
            $status = 0; try { $status = $_.Exception.Response.StatusCode.value__ } catch {}
            if ($status -eq 401 -or $status -eq 403 -or $status -eq 404) { return $all }
            if ($page -eq 1) { throw }  # First page failure is fatal
            break  # Partial results on later pages
        }
        if ($response.value -and $response.value.Count -gt 0) {
            $all.AddRange([object[]]$response.value)
        } else { break }
        $uri = $response.'@odata.nextLink'
    }
    return $all
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
    Initialize-Csv "$OutputPath/Flows.csv" @("FlowKey","FlowId","EnvironmentId","EnvironmentName","DisplayName","Description","State","CreatorObjectId","CreatorDisplayName","CreatedTime","LastModifiedTime","TriggerType","IsSolutionAware","SolutionId","IsManaged","SuspensionReason","CollectedAt")
    Initialize-Csv "$OutputPath/FlowTriggers.csv" @("FlowKey","FlowId","EnvironmentId","Position","Name","TriggerType","ConnectorId","OperationId","EndpointUrl","BaseUrl")
    Initialize-Csv "$OutputPath/FlowActions.csv" @("FlowKey","FlowId","EnvironmentId","Position","Name","ActionType","ConnectorId","OperationId","EndpointUrl","BaseUrl")
    Initialize-Csv "$OutputPath/FlowConnectionRefs.csv" @("FlowKey","FlowId","EnvironmentId","ConnectorId","ConnectionName","ConnectionUrl")
    Initialize-Csv "$OutputPath/Connectors.csv" @("ConnectorId","EnvironmentId","EnvironmentName","DisplayName","Description","Publisher","Tier","IsCustom","IconUri","CollectedAt")
    Initialize-Csv "$OutputPath/Connections.csv" @("ConnectionId","ConnectorId","EnvironmentId","EnvironmentName","DisplayName","ConnectionUrl","CreatedByObjectId","CreatedByName","CreatedByEmail","CreatedTime","Status","IsShared","CollectedAt")
    Initialize-Csv "$OutputPath/CopilotAgents.csv" @("AgentKey","BotId","EnvironmentId","EnvironmentName","DisplayName","SchemaName","AgentType","Language","AuthenticationMode","AuthenticationTrigger","AccessControlPolicy","RuntimeProvider","SupportedLanguages","State","StatusReason","PublishedOn","PublishedByName","Origin","Template","IsManaged","SolutionId","Configuration","CreatedOn","CreatedByName","ModifiedOn","ModifiedByName","TopicCount","KnowledgeSourceCount","SkillCount","CustomGPTCount","TotalComponents","CollectedAt")
    Initialize-Csv "$OutputPath/CopilotComponents.csv" @("ComponentId","AgentKey","BotId","BotName","EnvironmentId","EnvironmentName","Name","ComponentType","Category","Description","Status","IsManaged","CreatedOn","ModifiedOn","CollectedAt")
    # Clear checkpoint for fresh run
    if (Test-Path $checkpointFile) { Remove-Item $checkpointFile }
}

$totalApps = 0; $totalFlows = 0; $totalConnectors = 0; $totalConnections = 0
$totalAppConnRefs = 0; $totalTriggers = 0; $totalActions = 0; $totalFlowConnRefs = 0
$totalAgents = 0; $totalAgentComponents = 0
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

    # --- CONNECTORS & CONNECTIONS (fetched first to build URL lookups for apps and flows) ---
    $connBaseUrls = @{}   # connectionName -> URL (exact match)
    $envConnByType = @{}  # connectorId -> [list of unique URLs] (all connections for that connector type)
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

        Write-Host " Connections..." -ForegroundColor DarkGray -NoNewline
        $connections = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/connections?$apiVer" -Token $token
        $envConnCount = 0
        foreach ($c in $connections) {
            $envConnCount++
            if ($envConnCount % 1000 -eq 0) {
                Write-Host "$envConnCount..." -ForegroundColor DarkGray -NoNewline
            }
            $connId = $c.properties.apiId -replace '.*/apis/', ''
            $status = if ($c.properties.statuses -and $c.properties.statuses.Count -gt 0) { $c.properties.statuses[0].status } else { "Unknown" }

            # Extract connection URL from connectionParameters or connectionParametersSet
            $connUrl = ""
            $cp = $c.properties.connectionParameters
            if ($cp) {
                if ($cp.server) { $connUrl = $cp.server }
                if ($cp.database) { $connUrl = if ($connUrl) { "$connUrl/$($cp.database)" } else { $cp.database } }
                if (-not $connUrl -and $cp.workflowEndpoint) { $connUrl = $cp.workflowEndpoint }
                if (-not $connUrl -and $cp.siteUrl) { $connUrl = $cp.siteUrl }
                if (-not $connUrl -and $cp.'token:siteUrl') { $connUrl = $cp.'token:siteUrl' }
                if (-not $connUrl -and $cp.gateway) { $connUrl = $cp.gateway }
                if (-not $connUrl -and $cp.url) { $connUrl = $cp.url }
                if (-not $connUrl -and $cp.serviceUrl) { $connUrl = $cp.serviceUrl }
                if (-not $connUrl -and $cp.endpoint) { $connUrl = $cp.endpoint }
                if (-not $connUrl -and $cp.baseUrl) { $connUrl = $cp.baseUrl }
                if (-not $connUrl -and $cp.baseResourceUrl) { $connUrl = $cp.baseResourceUrl }
                if (-not $connUrl -and $cp.'token:baseResourceUrl') { $connUrl = $cp.'token:baseResourceUrl' }
                if (-not $connUrl -and $cp.resourceUrl) { $connUrl = $cp.resourceUrl }
            }
            if (-not $connUrl -and $c.properties.connectionParametersSet) {
                $vals = $c.properties.connectionParametersSet.values
                if ($vals) {
                    if ($vals.server.value) { $connUrl = $vals.server.value }
                    elseif ($vals.siteUrl.value) { $connUrl = $vals.siteUrl.value }
                    elseif ($vals.url.value) { $connUrl = $vals.url.value }
                    elseif ($vals.baseResourceUrl.value) { $connUrl = $vals.baseResourceUrl.value }
                    elseif ($vals.resourceUrl.value) { $connUrl = $vals.resourceUrl.value }
                }
            }
            # Fallback: derive Dataverse URL from environment OrgUrl
            if (-not $connUrl -and $connId -match 'commondataservice' -and $env.OrgUrl -and "$($env.OrgUrl)" -ne '') {
                $connUrl = "$($env.OrgUrl)".TrimEnd('/')
            }

            # Build lookups for cross-referencing with apps and flows
            if ($connUrl) {
                $connUrlStr = [string]$connUrl
                $connBaseUrls[$c.name] = $connUrlStr
                # Group all URLs by connector type (e.g. shared_sharepointonline -> all SP site URLs)
                if (-not $envConnByType.ContainsKey($connId)) {
                    $envConnByType[$connId] = [System.Collections.Generic.List[string]]::new()
                }
                if ($envConnByType[$connId] -notcontains $connUrlStr) {
                    $envConnByType[$connId].Add($connUrlStr)
                }
            }

            Append-CsvRow "$OutputPath/Connections.csv" ([PSCustomObject]@{
                ConnectionId=$c.name; ConnectorId=$connId; EnvironmentId=$envId; EnvironmentName=$env.DisplayName
                DisplayName=$c.properties.displayName; ConnectionUrl=$connUrl
                CreatedByObjectId=$c.properties.createdBy.id
                CreatedByName=$c.properties.createdBy.displayName; CreatedByEmail=$c.properties.createdBy.email
                CreatedTime=$c.properties.createdTime; Status=$status; IsShared=$c.properties.allowSharing; CollectedAt=$timestamp
            })
            $totalConnections++
        }
        Write-Host " $envConnCount ($($connBaseUrls.Count) with URLs)" -ForegroundColor DarkGray -NoNewline

        # --- Derive Dataverse URLs from environment OrgUrl ---
        # Admin API strips connectionParameters, but for Dataverse the URL is always the env OrgUrl
        $dataverseResolved = 0
        if ($env.OrgUrl -and "$($env.OrgUrl)" -ne '') {
            $orgUrl = "$($env.OrgUrl)".TrimEnd('/')
            foreach ($c in $connections) {
                $cId = $c.properties.apiId -replace '.*/apis/', ''
                if ($cId -match 'commondataservice' -and -not $connBaseUrls.ContainsKey($c.name)) {
                    $connBaseUrls[$c.name] = $orgUrl
                    $dataverseResolved++
                    if (-not $envConnByType.ContainsKey($cId)) {
                        $envConnByType[$cId] = [System.Collections.Generic.List[string]]::new()
                    }
                    if ($envConnByType[$cId] -notcontains $orgUrl) {
                        $envConnByType[$cId].Add($orgUrl)
                    }
                }
            }
            if ($dataverseResolved -gt 0) {
                Write-Host " Dataverse:$dataverseResolved→$orgUrl" -ForegroundColor DarkGray -NoNewline
            }
        }

        # --- Pass 2: Non-admin endpoint for connections to get connectionParameters ---
        # The admin endpoint strips connectionParameters. The non-admin endpoint returns
        # full details (including base URLs) for connections the caller can access.
        # Try connectors that typically have user-configurable URLs.
        $urlConnections = @($connections | Where-Object {
            -not $connBaseUrls.ContainsKey($_.name)
        } | Where-Object {
            $cId = $_.properties.apiId -replace '.*/apis/', ''
            $cId -match 'sendhttp|webcontents|httpwithazuread|httpwebhook|sharepointonline|sql|azuresql|azureblob|azurequeues|azuretables|azurefile|documentdb|dynamicscrmonline'
        })

        if ($urlConnections.Count -gt 0) {
            # Cap at 200 to avoid excessive API calls (non-admin endpoint fails for others' connections)
            $maxPass2 = [math]::Min($urlConnections.Count, 200)
            $urlConnections = @($urlConnections | Select-Object -First $maxPass2)
            Write-Host " Conn detail ($($urlConnections.Count))..." -ForegroundColor DarkGray -NoNewline
            $pass2Resolved = 0
            foreach ($hc in $urlConnections) {
                $hcApiId = $hc.properties.apiId -replace '.*/apis/', ''
                $hcName = $hc.name
                try {
                    $detailUri = "$pa/providers/Microsoft.PowerApps/apis/$hcApiId/connections/${hcName}?$apiVer&`$filter=environment eq '$envId'"
                    $detailConn = Invoke-RestMethod -Uri $detailUri -Method Get -Headers @{
                        "Authorization" = "Bearer $token"; "Accept" = "application/json"
                    } -TimeoutSec 30
                    if ($detailConn -and $detailConn.properties) {
                        $cp = $detailConn.properties.connectionParameters
                        $hcUrl = ""
                        if ($cp) {
                            foreach ($k in @('baseResourceUrl', 'token:baseResourceUrl', 'baseUrl', 'resourceUrl',
                                             'serviceUrl', 'url', 'siteUrl', 'token:siteUrl', 'server',
                                             'dataset', 'endpoint', 'gateway')) {
                                if ($cp.PSObject.Properties.Name -contains $k) {
                                    $v = "$($cp.$k)"
                                    if ($v -and $v -ne '') {
                                        # For server+database, combine them
                                        if ($k -eq 'server' -and $cp.PSObject.Properties.Name -contains 'database') {
                                            $db = "$($cp.database)"
                                            if ($db -and $db -ne '') { $v = "$v/$db" }
                                        }
                                        $hcUrl = $v; break
                                    }
                                }
                            }
                            if (-not $hcUrl) {
                                # Scan all properties for a URL value
                                foreach ($p in $cp.PSObject.Properties) {
                                    $v = "$($p.Value)"
                                    if ($v -match '^https?://' -and $v -notmatch '\.(png|jpg|svg|gif|ico)') {
                                        $hcUrl = $v; break
                                    }
                                }
                            }
                        }
                        if ($hcUrl) {
                            $connBaseUrls[$hcName] = $hcUrl
                            $hcConnId = $hcApiId -replace '.*/apis/', ''
                            if (-not $envConnByType.ContainsKey($hcConnId)) {
                                $envConnByType[$hcConnId] = [System.Collections.Generic.List[string]]::new()
                            }
                            if ($envConnByType[$hcConnId] -notcontains $hcUrl) {
                                $envConnByType[$hcConnId].Add($hcUrl)
                            }
                            $pass2Resolved++
                        }
                    }
                }
                catch {
                    # Non-admin endpoint fails for other users' connections — expected
                }
            }
            Write-Host " $pass2Resolved resolved" -ForegroundColor DarkGray -NoNewline
        }
    }
    catch {
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Connectors"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
        Write-Host " Warning (connectors/connections): $($_.Exception.Message)" -ForegroundColor DarkYellow
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

            # Extract connection references — resolve URLs via connector type lookup
            if ($app.properties.connectionReferences) {
                foreach ($ref in $app.properties.connectionReferences.PSObject.Properties) {
                    $connId = $ref.Value.id -replace '.*/apis/', ''
                    # Resolve URL: try exact connection name match first, then all URLs for this connector type
                    $refUrl = ""
                    $appConnName = if ($ref.Value.connectionName) { $ref.Value.connectionName }
                                   elseif ($ref.Value.connection -and $ref.Value.connection.name) { $ref.Value.connection.name }
                                   else { "" }
                    if ($appConnName -and $connBaseUrls.ContainsKey($appConnName)) {
                        $refUrl = $connBaseUrls[$appConnName]
                    }
                    elseif ($connId -and $envConnByType.ContainsKey($connId)) {
                        $refUrl = $envConnByType[$connId] -join "; "
                    }
                    Append-CsvRow "$OutputPath/AppConnectorRefs.csv" ([PSCustomObject]@{
                        AppId=$app.name; EnvironmentId=$envId; ConnectorId=$connId
                        DisplayName=$ref.Value.displayName; DataSources=($ref.Value.dataSources -join "; ")
                        EndpointUrl=$refUrl
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
                            if ($res -and $res.Count -gt 0) { $flowDefCache[$pj.FlowId] = $res[0] }
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

            # Composite key for Power BI relationships (unique even if FlowId repeats across environments)
            $flowKey = "$($f.name)|$envId"

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

            # --- Helper: try to extract a URL string from an arbitrary value ---
            function Get-UrlString {
                param($Value)
                if ($null -eq $Value) { return $null }
                if ($Value -is [string]) {
                    if ($Value -match '^https?://') { return $Value }
                    return $null
                }
                # Object — stringify and check
                if ($null -ne $Value.PSObject) {
                    $s = "$Value"
                    if ($s -match '^https?://' -and $s -notmatch '^System\.') { return $s }
                }
                return $null
            }

            # --- Helper: recursively scan an object for any property whose key or value looks like a URL ---
            function Find-UrlInObject {
                param($Obj, [int]$Depth = 0)
                if ($Depth -gt 4 -or $null -eq $Obj) { return $null }
                if ($Obj -is [string]) { if ($Obj -match '^https?://') { return $Obj }; return $null }
                if ($Obj -is [int] -or $Obj -is [bool] -or $Obj -is [double]) { return $null }
                if ($null -eq $Obj.PSObject) { return $null }
                foreach ($p in $Obj.PSObject.Properties) {
                    $k = $p.Name.ToLower()
                    # Skip known non-URL properties to avoid false positives
                    if ($k -in @('host','authentication','retryPolicy','metadata','trackedProperties','operationOptions','type','kind','runtimeConfiguration')) { continue }
                    # Priority: keys that likely hold URLs
                    if ($k -match 'uri$|url$|endpoint|baseurl|serviceurl|siteurl|dataset|server') {
                        $v = Get-UrlString $p.Value
                        if ($v) { return $v }
                        # Might be non-URL but still useful (e.g. server name)
                        if ($p.Value -is [string] -and $p.Value -ne '') {
                            if ($k -match 'uri$|url$|endpoint|baseurl|serviceurl|siteurl') { return $p.Value }
                            if ($k -eq 'dataset') { return $p.Value }
                            if ($k -eq 'server') {
                                $sv = $p.Value
                                if ($Obj.PSObject.Properties.Name -contains 'database') { $sv = "$sv/$($Obj.database)" }
                                if ($sv -and $sv -ne '/' -and $sv -ne '') { return $sv }
                            }
                        }
                    }
                }
                # Second pass: recurse into sub-objects
                foreach ($p in $Obj.PSObject.Properties) {
                    $k = $p.Name.ToLower()
                    if ($k -in @('host','authentication','retryPolicy','metadata','trackedProperties','runtimeConfiguration')) { continue }
                    if ($p.Value -and $null -ne $p.Value.PSObject -and -not ($p.Value -is [string])) {
                        $found = Find-UrlInObject $p.Value ($Depth + 1)
                        if ($found) { return $found }
                    }
                }
                return $null
            }

            # --- Helper: extract endpoint URL from step input parameters ---
            # This is where the actual URLs live (SharePoint sites, SQL servers, HTTP endpoints)
            function Get-StepEndpointUrl {
                param($Inputs)
                if (-not $Inputs) { return "" }
                # Inputs can be a scalar (Compose actions) — only process objects
                if ($Inputs -is [string] -or $Inputs -is [int] -or $Inputs -is [bool] -or
                    $null -eq $Inputs.PSObject) { return "" }

                # --- Pass 1: check root-level uri/url (built-in Http action type) ---
                foreach ($uriKey in @('uri','url')) {
                    if ($Inputs.PSObject.Properties.Name -contains $uriKey) {
                        $v = Get-UrlString $Inputs.$uriKey
                        if ($v) { return $v }
                        # Even non-URL string (could be expression) — return it
                        if ($Inputs.$uriKey -is [string] -and $Inputs.$uriKey -ne '') { return $Inputs.$uriKey }
                    }
                }

                # --- Pass 2: check parameters (ApiConnection connectors) ---
                $params = $null
                if ($Inputs.PSObject.Properties.Name -contains 'parameters') { $params = $Inputs.parameters }
                if ($params -and $null -ne $params.PSObject -and -not ($params -is [string])) {
                    # Check exact keys first (uri, url)
                    foreach ($uriKey in @('uri','url')) {
                        if ($params.PSObject.Properties.Name -contains $uriKey) {
                            $v = Get-UrlString $params.$uriKey
                            if ($v) { return $v }
                            if ($params.$uriKey -is [string] -and $params.$uriKey -ne '') { return $params.$uriKey }
                        }
                    }
                    # Check slash-prefixed keys (Power Automate convention: request/uri, request/url, etc.)
                    foreach ($p in $params.PSObject.Properties) {
                        if ($p.Name -match '/uri$|/url$|/endpoint$|/baseUrl$|/serviceUrl$') {
                            $v = Get-UrlString $p.Value
                            if ($v) { return $v }
                            if ($p.Value -is [string] -and $p.Value -ne '') { return $p.Value }
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
                    # Generic URL fields (exact and slash-prefixed)
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
                }

                # --- Pass 3: deep scan the entire inputs tree for any URL ---
                $found = Find-UrlInObject $Inputs
                if ($found) { return $found }

                return ""
            }

            # --- Helper: extract base URL from step input parameters ---
            # Many connectors store the base URL (site, server, host) separately from the
            # relative endpoint path. E.g. SharePoint: dataset = site URL, uri = relative path.
            # HTTP with Azure AD: baseResourceUrl in connection, uri = relative path.
            function Get-StepBaseUrl {
                param($Inputs)
                if (-not $Inputs) { return "" }
                if ($Inputs -is [string] -or $Inputs -is [int] -or $Inputs -is [bool] -or
                    $null -eq $Inputs.PSObject) { return "" }

                $params = $null
                if ($Inputs.PSObject.Properties.Name -contains 'parameters') { $params = $Inputs.parameters }
                if (-not $params -or $null -eq $params.PSObject -or ($params -is [string])) { return "" }

                # Check well-known base URL parameter names (these hold the host/site, not the endpoint path)
                foreach ($key in @('dataset', 'server', 'siteUrl', 'token:siteUrl', 'serviceUrl',
                                   'baseUrl', 'baseResourceUrl', 'token:baseResourceUrl', 'resourceUrl',
                                   'hostname', 'hostName', 'endpoint', 'workflowEndpoint', 'gateway')) {
                    if ($params.PSObject.Properties.Name -contains $key) {
                        $v = "$($params.$key)"
                        if ($v -and $v -ne '') {
                            # For server+database combo (SQL), append database
                            if ($key -eq 'server' -and $params.PSObject.Properties.Name -contains 'database') {
                                $db = "$($params.database)"
                                if ($db -and $db -ne '') { $v = "$v/$db" }
                            }
                            return $v
                        }
                    }
                }
                # Check slash-prefixed keys (Power Automate convention)
                foreach ($p in $params.PSObject.Properties) {
                    if ($p.Name -match '/dataset$|/siteUrl$|/server$|/baseUrl$|/serviceUrl$|/baseResourceUrl$') {
                        $v = "$($p.Value)"
                        if ($v -and $v -ne '') { return $v }
                    }
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
                param($ActionsObj, [string]$FlowKey, [string]$FlowId, [string]$EnvId, [string]$OutPath, [ref]$Pos, [ref]$Count, [hashtable]$BaseUrls)
                if (-not $ActionsObj -or $null -eq $ActionsObj.PSObject) { return }
                foreach ($prop in $ActionsObj.PSObject.Properties) {
                    $stepName = $prop.Name
                    $step = $prop.Value
                    if (-not $step -or $null -eq $step.PSObject) { continue }
                    $stepType = if ($step.PSObject.Properties.Name -contains 'type') { $step.type } else { "Unknown" }
                    $inputs = if ($step.PSObject.Properties.Name -contains 'inputs') { $step.inputs } else { $null }
                    $hostInfo = Get-StepHostInfo $inputs
                    $epUrl = Get-StepEndpointUrl $inputs

                    # Extract base URL: 1) from action parameters, 2) from connection lookup, 3) from connector type lookup, 4) from endpoint URL
                    $baseUrl = Get-StepBaseUrl $inputs
                    if ($baseUrl -eq '' -and $BaseUrls -and $hostInfo.ConnectionName -and $BaseUrls.ContainsKey($hostInfo.ConnectionName)) {
                        $baseUrl = $BaseUrls[$hostInfo.ConnectionName]
                    }
                    if ($baseUrl -eq '' -and $hostInfo.ConnectorId -and $envConnByType.ContainsKey($hostInfo.ConnectorId)) {
                        $baseUrl = $envConnByType[$hostInfo.ConnectorId] -join "; "
                    }
                    if ($baseUrl -eq '' -and $epUrl -match '^(https?://[^/]+)') {
                        $baseUrl = $matches[1]
                    }

                    # Debug: dump HTTP connector inputs to file when endpoint is empty
                    if ($epUrl -eq '' -and $hostInfo.ConnectorId -match 'http|sendhttp|webcontents|httpwithazuread|httpwebhook') {
                        $debugFile = "$OutPath/_debug_http.log"
                        $debugLine = "=== Flow=$FlowId Step=$stepName Connector=$($hostInfo.ConnectorId) Type=$stepType ==="
                        Write-Host "      [DEBUG-HTTP] $stepName ($($hostInfo.ConnectorId)) — empty URL, see _debug_http.log" -ForegroundColor Magenta
                        try {
                            $inputJson = if ($inputs) { $inputs | ConvertTo-Json -Depth 10 } else { "NULL" }
                            "$debugLine`n$inputJson`n" | Add-Content -Path $debugFile -Encoding UTF8
                        } catch {
                            "Failed to serialize: $($_.Exception.Message)" | Add-Content -Path $debugFile -Encoding UTF8
                        }
                    }

                    Append-CsvRow "$OutPath/FlowActions.csv" ([PSCustomObject]@{
                        FlowKey=$FlowKey; FlowId=$FlowId; EnvironmentId=$EnvId; Position=$Pos.Value; Name=$stepName
                        ActionType=$stepType; ConnectorId=$hostInfo.ConnectorId
                        OperationId=$hostInfo.OperationId; EndpointUrl=$epUrl; BaseUrl=$baseUrl
                    })
                    $Pos.Value++
                    $Count.Value++

                    # Recurse into nested actions: Scope, ForEach, Until have .actions
                    if ($step.PSObject.Properties.Name -contains 'actions' -and $step.actions) {
                        Write-FlattenedActions $step.actions $FlowKey $FlowId $EnvId $OutPath $Pos $Count $BaseUrls
                    }
                    # Condition: .else.actions
                    if ($step.PSObject.Properties.Name -contains 'else' -and $step.else -and
                        $step.else.PSObject.Properties.Name -contains 'actions' -and $step.else.actions) {
                        Write-FlattenedActions $step.else.actions $FlowKey $FlowId $EnvId $OutPath $Pos $Count $BaseUrls
                    }
                    # Switch: .cases.*.actions and .default.actions
                    if ($step.PSObject.Properties.Name -contains 'cases' -and $step.cases) {
                        foreach ($caseProp in $step.cases.PSObject.Properties) {
                            if ($caseProp.Value.PSObject.Properties.Name -contains 'actions' -and $caseProp.Value.actions) {
                                Write-FlattenedActions $caseProp.Value.actions $FlowKey $FlowId $EnvId $OutPath $Pos $Count $BaseUrls
                            }
                        }
                    }
                    if ($step.PSObject.Properties.Name -contains 'default' -and $step.default -and
                        $step.default.PSObject.Properties.Name -contains 'actions' -and $step.default.actions) {
                        Write-FlattenedActions $step.default.actions $FlowKey $FlowId $EnvId $OutPath $Pos $Count $BaseUrls
                    }
                }
            }

            # --- Write FlowConnectionRefs and extract base URLs from connectionReferences ---
            if ($connRefs) {
                foreach ($ref in $connRefs.PSObject.Properties) {
                    $crConnId = if ($ref.Value.PSObject.Properties.Name -contains 'id') { $ref.Value.id -replace '.*/apis/', '' }
                                elseif ($ref.Value.PSObject.Properties.Name -contains 'api' -and $ref.Value.api.name) { $ref.Value.api.name }
                                else { $ref.Name }
                    $crConnName = if ($ref.Value.PSObject.Properties.Name -contains 'connectionName') { $ref.Value.connectionName }
                                  else { "" }

                    # Resolve URL from environment connections lookup
                    $crBaseUrl = ""
                    if ($crConnName -and $connBaseUrls.ContainsKey($crConnName)) {
                        $crBaseUrl = $connBaseUrls[$crConnName]
                    } elseif ($crConnId -and $envConnByType.ContainsKey($crConnId)) {
                        $crBaseUrl = $envConnByType[$crConnId] -join "; "
                    }

                    Append-CsvRow "$OutputPath/FlowConnectionRefs.csv" ([PSCustomObject]@{
                        FlowKey=$flowKey; FlowId=$f.name; EnvironmentId=$envId; ConnectorId=$crConnId; ConnectionName=$crConnName; ConnectionUrl=$crBaseUrl
                    })
                    $totalFlowConnRefs++

                    # Debug: dump HTTP connection references to file
                    if ($crConnId -match 'http|sendhttp|webcontents|httpwithazuread|httpwebhook') {
                        $debugConnFile = "$OutputPath/_debug_connrefs.log"
                        try {
                            $refJson = $ref.Value | ConvertTo-Json -Depth 10
                            "=== Flow=$($f.name) RefName=$($ref.Name) ConnId=$crConnId ConnName=$crConnName BaseUrl=$crBaseUrl ===`n$refJson`n" | Add-Content -Path $debugConnFile -Encoding UTF8
                        } catch {
                            "=== Flow=$($f.name) RefName=$($ref.Name) — serialize failed ===" | Add-Content -Path $debugConnFile -Encoding UTF8
                        }
                    }
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
                        # Extract base URL: 1) from trigger parameters, 2) from connection lookup, 3) from connector type lookup, 4) from endpoint URL
                        $baseUrl = Get-StepBaseUrl $inputs
                        if ($baseUrl -eq '' -and $hostInfo.ConnectionName -and $connBaseUrls.ContainsKey($hostInfo.ConnectionName)) {
                            $baseUrl = $connBaseUrls[$hostInfo.ConnectionName]
                        }
                        if ($baseUrl -eq '' -and $hostInfo.ConnectorId -and $envConnByType.ContainsKey($hostInfo.ConnectorId)) {
                            $baseUrl = $envConnByType[$hostInfo.ConnectorId] -join "; "
                        }
                        if ($baseUrl -eq '' -and $epUrl -match '^(https?://[^/]+)') {
                            $baseUrl = $matches[1]
                        }
                        if ($pos -eq 0) { $triggerType = $tType }
                        Append-CsvRow "$OutputPath/FlowTriggers.csv" ([PSCustomObject]@{
                            FlowKey=$flowKey; FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=$stepName
                            TriggerType=$tType; ConnectorId=$hostInfo.ConnectorId
                            OperationId=$hostInfo.OperationId; EndpointUrl=$epUrl; BaseUrl=$baseUrl
                        })
                        $pos++; $totalTriggers++
                    }

                    if ($flowDefinition.PSObject.Properties.Name -contains 'actions' -and $flowDefinition.actions) {
                        $actionPos = [ref]0
                        $actionCount = [ref]0
                        Write-FlattenedActions $flowDefinition.actions $flowKey $f.name $envId $OutputPath $actionPos $actionCount $connBaseUrls
                        $totalActions += $actionCount.Value
                    }
                }

                # FALLBACK: Parse from definitionSummary (no input parameters, but we can
                # resolve BaseUrl from connection references + connBaseUrls/envConnByType)
                if (-not $usedFullDef -and $defSummary) {
                    # Build connector-to-URL lookup from this flow's connection references
                    $connIdToUrl = @{}
                    if ($connRefs) {
                        foreach ($ref in $connRefs.PSObject.Properties) {
                            $crConnId = ""
                            if ($ref.Value.PSObject.Properties.Name -contains 'id') {
                                $crConnId = $ref.Value.id -replace '.*/apis/', ''
                            } elseif ($ref.Value.PSObject.Properties.Name -contains 'api' -and $ref.Value.api.name) {
                                $crConnId = $ref.Value.api.name
                            }
                            if (-not $crConnId) { continue }
                            $crConnName = if ($ref.Value.PSObject.Properties.Name -contains 'connectionName') { $ref.Value.connectionName } else { "" }
                            $crUrl = ""
                            if ($crConnName -and $connBaseUrls.ContainsKey($crConnName)) {
                                $crUrl = $connBaseUrls[$crConnName]
                            } elseif ($crConnId -and $envConnByType.ContainsKey($crConnId)) {
                                $crUrl = $envConnByType[$crConnId] -join "; "
                            }
                            if ($crUrl -and -not $connIdToUrl.ContainsKey($crConnId)) {
                                $connIdToUrl[$crConnId] = $crUrl
                            }
                        }
                    }

                    if ($defSummary.triggers) {
                        $pos = 0
                        foreach ($t in $defSummary.triggers) {
                            $tConnId = if ($t.api -and $t.api.id) { $t.api.id -replace '.*/apis/', '' } else { "" }
                            if ($pos -eq 0) { $triggerType = $t.type }
                            $tBaseUrl = if ($tConnId -and $connIdToUrl.ContainsKey($tConnId)) { $connIdToUrl[$tConnId] } else { "" }
                            Append-CsvRow "$OutputPath/FlowTriggers.csv" ([PSCustomObject]@{
                                FlowKey=$flowKey; FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                                TriggerType=$t.type; ConnectorId=$tConnId; OperationId=$t.swaggerOperationId; EndpointUrl=""; BaseUrl=$tBaseUrl
                            })
                            $pos++; $totalTriggers++
                        }
                    }
                    if ($defSummary.actions) {
                        $pos = 0
                        foreach ($a in $defSummary.actions) {
                            $aConnId = if ($a.api -and $a.api.id) { $a.api.id -replace '.*/apis/', '' } else { "" }
                            $aBaseUrl = if ($aConnId -and $connIdToUrl.ContainsKey($aConnId)) { $connIdToUrl[$aConnId] } else { "" }
                            Append-CsvRow "$OutputPath/FlowActions.csv" ([PSCustomObject]@{
                                FlowKey=$flowKey; FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                                ActionType=$a.type; ConnectorId=$aConnId; OperationId=$a.swaggerOperationId; EndpointUrl=""; BaseUrl=$aBaseUrl
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
                FlowKey=$flowKey; FlowId=$f.name; EnvironmentId=$envId; EnvironmentName=$env.DisplayName
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

    # --- COPILOT STUDIO AGENTS (requires Dataverse-enabled environment) ---
    if ($env.OrgUrl -and "$($env.OrgUrl)" -ne '') {
        try {
            Write-Host "    Copilot agents..." -ForegroundColor DarkGray -NoNewline
            $dvToken = Get-DataverseToken $env.OrgUrl
            if ($dvToken) {
                $fv = 'OData.Community.Display.V1.FormattedValue'
                $componentTypeLabels = @{
                    0='Topic'; 1='Skill'; 2='Bot Variable'; 3='Bot Entity'; 4='Dialog'; 5='Trigger'
                    6='Language Understanding'; 7='Language Generation'; 8='Dialog Schema'; 9='Topic'
                    10='Bot Translations'; 11='Bot Entity'; 12='Bot Variable'; 13='Skill'
                    14='File Attachment'; 15='Custom GPT'; 16='Knowledge Source'; 17='External Trigger'
                    18='Copilot Settings'; 19='Test Case'
                }

                # DIAGNOSTIC PROBE: simplest possible query to see if bot entity is accessible
                # This isolates auth/schema issues from the main query
                $probeOk = $false
                $probeCount = -1
                $probeError = $null
                try {
                    $probe = Invoke-DataverseOData -OrgUrl $env.OrgUrl -Token $dvToken -Query 'bots?$select=botid,name&$top=5'
                    $probeOk = $true
                    $probeCount = @($probe).Count
                    Write-Host " [probe:$probeCount]" -ForegroundColor DarkCyan -NoNewline
                    if ($probeCount -gt 0) {
                        $firstName = "$($probe[0].name)"
                        if ($firstName) { Write-Host " first='$firstName'" -ForegroundColor DarkCyan -NoNewline }
                    }
                }
                catch {
                    $probeError = $_.Exception.Message
                    $status = 0; try { $status = $_.Exception.Response.StatusCode.value__ } catch {}
                    Write-Host " [probe-failed:$status $probeError]" -ForegroundColor DarkYellow -NoNewline
                }

                # Fetch all bots in this environment
                # Strategy: probe first (2 fields), then try curated field list, then fall back
                # to no-$select (all default fields), then finally use probe data.
                # The bot entity column set varies across Dataverse versions — fields like
                # accesscontrolpolicy, origin, _solutionid_value are NOT standard and break $select.
                $bots = $null
                $botQueryError = $null
                if ($probeOk -and $probeCount -gt 0) {
                    # Try 1: curated safe field list (confirmed in PowerDocu bot.xml schema)
                    try {
                        $bots = Invoke-DataverseOData -OrgUrl $env.OrgUrl -Token $dvToken -Query (
                            'bots?$select=botid,name,schemaname,language,authenticationmode,authenticationtrigger,' +
                            'runtimeprovider,statecode,statuscode,publishedon,_publishedby_value,' +
                            'template,ismanaged,iscustomizable,' +
                            'createdon,_createdby_value,modifiedon,_modifiedby_value'
                        )
                    }
                    catch {
                        $botQueryError = "curated: $($_.Exception.Message)"
                        # Try 2: no $select at all — returns all default fields, guaranteed safe
                        try {
                            $bots = Invoke-DataverseOData -OrgUrl $env.OrgUrl -Token $dvToken -Query 'bots'
                            if ($bots.Count -gt 0) {
                                Write-Host " (no-select)" -ForegroundColor DarkGray -NoNewline
                                $botQueryError = $null
                            }
                        }
                        catch {
                            $botQueryError += " | no-select: $($_.Exception.Message)"
                            # Try 3: use probe data directly (botid + name only)
                            $bots = $probe
                            Write-Host " (using-probe-data)" -ForegroundColor DarkYellow -NoNewline
                            $botQueryError = $null
                        }
                    }
                }
                if (-not $bots) { $bots = @() }
                $envAgentCount = 0
                $allComponents = @()

                if ($bots -and $bots.Count -gt 0) {
                    # Fetch ALL botcomponents for this environment at once (more efficient than per-bot)
                    $allComponents = Invoke-DataverseOData -OrgUrl $env.OrgUrl -Token $dvToken -Query (
                        'botcomponents?$select=botcomponentid,name,componenttype,category,description,' +
                        'statecode,ismanaged,createdon,modifiedon,_parentbotid_value'
                    )
                    # Group components by parent bot ID
                    $compsByBot = @{}
                    foreach ($comp in $allComponents) {
                        $parentId = "$($comp._parentbotid_value)"
                        if ($parentId -and $parentId -ne '') {
                            if (-not $compsByBot.ContainsKey($parentId)) {
                                $compsByBot[$parentId] = [System.Collections.Generic.List[object]]::new()
                            }
                            $compsByBot[$parentId].Add($comp)
                        }
                    }

                    foreach ($bot in $bots) {
                        $botId = "$($bot.botid)"
                        $botName = "$($bot.name)"

                        # Resolve formatted values (picklist labels)
                        $fvLang = "language@$fv"; $langLabel = "$($bot.$fvLang)"
                        if (-not $langLabel) { $langLabel = "$($bot.language)" }
                        $fvAuth = "authenticationmode@$fv"; $authLabel = "$($bot.$fvAuth)"
                        if (-not $authLabel) {
                            $authLabel = switch ($bot.authenticationmode) { 0 {"Unspecified"} 1 {"None"} 2 {"Integrated"} 3 {"Custom Azure AD"} 4 {"Generic OAuth2"} default {"$($bot.authenticationmode)"} }
                        }
                        $fvAcp = "accesscontrolpolicy@$fv"; $acpLabel = "$($bot.$fvAcp)"
                        if (-not $acpLabel) {
                            $acpLabel = switch ($bot.accesscontrolpolicy) { 0 {"Any"} 1 {"Copilot readers"} 2 {"Group membership"} 3 {"Any (multi-tenant)"} default {"$($bot.accesscontrolpolicy)"} }
                        }
                        $stateLabel = if ($bot.statecode -eq 0) { "Active" } else { "Inactive" }
                        $fvStatus = "statuscode@$fv"; $statusLabel = "$($bot.$fvStatus)"
                        if (-not $statusLabel) {
                            $statusLabel = switch ($bot.statuscode) { 1 {"Provisioned"} 2 {"Deprovisioned"} 3 {"Provisioning"} 4 {"ProvisionFailed"} 5 {"MissingLicense"} default {"$($bot.statuscode)"} }
                        }

                        # Get component counts for this bot
                        $botComps = if ($compsByBot.ContainsKey($botId)) { $compsByBot[$botId] } else { @() }
                        $topicCount = @($botComps | Where-Object { $_.componenttype -eq 0 -or $_.componenttype -eq 9 }).Count
                        $knowledgeCount = @($botComps | Where-Object { $_.componenttype -eq 16 }).Count
                        $skillCount = @($botComps | Where-Object { $_.componenttype -eq 1 -or $_.componenttype -eq 13 }).Count
                        $customGptCount = @($botComps | Where-Object { $_.componenttype -eq 15 }).Count

                        # Classify agent type: Declarative (lite/Agent Builder) vs Custom (full Copilot Studio)
                        # Agents with a Custom GPT component (componenttype=15) are declarative/lite agents
                        $agentType = if ($customGptCount -gt 0) { "Declarative" } else { "Custom" }

                        # Resolve additional picklist labels
                        $fvAuthTrig = "authenticationtrigger@$fv"; $authTrigLabel = "$($bot.$fvAuthTrig)"
                        if (-not $authTrigLabel) {
                            $authTrigLabel = switch ($bot.authenticationtrigger) { 0 {"As Needed"} 1 {"Always"} default {"$($bot.authenticationtrigger)"} }
                        }
                        $fvRuntime = "runtimeprovider@$fv"; $runtimeLabel = "$($bot.$fvRuntime)"
                        if (-not $runtimeLabel) {
                            $runtimeLabel = switch ($bot.runtimeprovider) { 0 {"Power Virtual Agents"} 1 {"Nuance Mix Shell"} default {"$($bot.runtimeprovider)"} }
                        }

                        # SupportedLanguages is a multi-select picklist; grab formatted value or raw
                        $fvSuppLang = "supportedlanguages@$fv"; $suppLangLabel = "$($bot.$fvSuppLang)"
                        if (-not $suppLangLabel) { $suppLangLabel = "$($bot.supportedlanguages)" }

                        # Configuration is a JSON memo field — include as-is
                        $configVal = "$($bot.configuration)"

                        # Composite key: botid is not unique across environments (system-provisioned
                        # bots like "Copilot in Power Apps" share GUIDs), so we use envId_botId.
                        $agentKey = "$envId`_$botId"

                        Append-CsvRow "$OutputPath/CopilotAgents.csv" ([PSCustomObject]@{
                            AgentKey = $agentKey
                            BotId = $botId
                            EnvironmentId = $envId
                            EnvironmentName = $env.DisplayName
                            DisplayName = $botName
                            SchemaName = "$($bot.schemaname)"
                            AgentType = $agentType
                            Language = $langLabel
                            AuthenticationMode = $authLabel
                            AuthenticationTrigger = $authTrigLabel
                            AccessControlPolicy = $acpLabel
                            RuntimeProvider = $runtimeLabel
                            SupportedLanguages = $suppLangLabel
                            State = $stateLabel
                            StatusReason = $statusLabel
                            PublishedOn = "$($bot.publishedon)"
                            PublishedByName = $(
                                $fvPub = "_publishedby_value@$fv"; $v = "$($bot.$fvPub)"
                                if ($v) { $v } else { "" }
                            )
                            Origin = "$($bot.origin)"
                            Template = "$($bot.template)"
                            IsManaged = "$($bot.ismanaged)"
                            SolutionId = "$($bot._solutionid_value)"
                            Configuration = $configVal
                            CreatedOn = "$($bot.createdon)"
                            CreatedByName = $(
                                $fvCre = "_createdby_value@$fv"; $v = "$($bot.$fvCre)"
                                if ($v) { $v } else { "" }
                            )
                            ModifiedOn = "$($bot.modifiedon)"
                            ModifiedByName = $(
                                $fvMod = "_modifiedby_value@$fv"; $v = "$($bot.$fvMod)"
                                if ($v) { $v } else { "" }
                            )
                            TopicCount = $topicCount
                            KnowledgeSourceCount = $knowledgeCount
                            SkillCount = $skillCount
                            CustomGPTCount = $customGptCount
                            TotalComponents = $botComps.Count
                            CollectedAt = $timestamp
                        })
                        $envAgentCount++
                        $totalAgents++

                        # Write individual components
                        foreach ($comp in $botComps) {
                            $ctVal = $comp.componenttype
                            $fvCt = "componenttype@$fv"; $ctLabel = "$($comp.$fvCt)"
                            if (-not $ctLabel -and $componentTypeLabels.ContainsKey([int]$ctVal)) {
                                $ctLabel = $componentTypeLabels[[int]$ctVal]
                            }
                            if (-not $ctLabel) { $ctLabel = "$ctVal" }
                            $compState = if ($comp.statecode -eq 0) { "Active" } else { "Inactive" }

                            Append-CsvRow "$OutputPath/CopilotComponents.csv" ([PSCustomObject]@{
                                ComponentId = "$($comp.botcomponentid)"
                                AgentKey = $agentKey
                                BotId = $botId
                                BotName = $botName
                                EnvironmentId = $envId
                                EnvironmentName = $env.DisplayName
                                Name = "$($comp.name)"
                                ComponentType = $ctLabel
                                Category = "$($comp.category)"
                                Description = "$($comp.description)"
                                Status = $compState
                                IsManaged = "$($comp.ismanaged)"
                                CreatedOn = "$($comp.createdon)"
                                ModifiedOn = "$($comp.modifiedon)"
                                CollectedAt = $timestamp
                            })
                            $totalAgentComponents++
                        }
                    }
                }
                if ($botQueryError -and $bots.Count -eq 0) {
                    Write-Host " query error: $botQueryError" -ForegroundColor DarkYellow
                } else {
                    Write-Host " $envAgentCount agents, $($allComponents.Count) components" -ForegroundColor DarkGray
                }
            }
            else {
                Write-Host " skipped (no Dataverse token)" -ForegroundColor DarkGray
            }
        }
        catch {
            $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="CopilotAgents"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
            Write-Host " Warning (copilot): $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
    }

    # Mark environment as completed in checkpoint
    Add-Content -Path $checkpointFile -Value $envId -Encoding UTF8
    [void]$completedEnvs.Add($envId)
}

Write-Host "  Totals: $totalApps apps, $totalFlows flows, $totalConnectors connectors, $totalConnections connections" -ForegroundColor Gray
Write-Host "  Totals: $totalAppConnRefs app-connector refs, $totalFlowConnRefs flow-connector refs, $totalTriggers triggers, $totalActions actions" -ForegroundColor Gray
Write-Host "  Totals: $totalAgents copilot agents, $totalAgentComponents agent components" -ForegroundColor Gray

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
# Try multiple endpoint/token/API-version combinations for usage analytics
$usageAttempts = @(
    @{ Name="BAP+BapToken";   Uri="$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?$apiVer"; Token={ Get-BapToken } }
    @{ Name="BAP+PPToken";    Uri="$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?$apiVer"; Token={ Get-PPToken } }
    @{ Name="BAP+AdminToken"; Uri="$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?$apiVer"; Token={ Get-AdminToken } }
    @{ Name="PP-API+Admin";   Uri="https://api.powerplatform.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?api-version=2022-03-01-preview"; Token={ Get-AdminToken } }
    @{ Name="BAP+v2021";      Uri="$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/analytics/usage?api-version=2021-04-01"; Token={ Get-BapToken } }
)
foreach ($attempt in $usageAttempts) {
    if ($usageCollected) { break }
    try {
        $uToken = & $attempt.Token
        $usage = Invoke-PPApiPaged -Uri $attempt.Uri -Token $uToken
        if ($usage -and $usage.Count -gt 0) {
            $allUsage = $usage | ForEach-Object {
                [PSCustomObject]@{
                    ResourceType=$_.resourceType; EnvironmentId=$_.environmentId; Date=$_.date
                    UniqueUsers=$_.uniqueUsers; TotalSessions=$_.totalSessions; TotalActions=$_.totalActions; CollectedAt=$timestamp
                }
            }
            $allUsage | Export-Csv "$OutputPath/UsageAnalytics.csv" -NoTypeInformation
            Write-Host "  Found $($allUsage.Count) usage records (via $($attempt.Name))" -ForegroundColor Gray
            $usageCollected = $true
        }
        else {
            Write-Host "  $($attempt.Name): empty response" -ForegroundColor DarkGray
        }
    }
    catch {
        Write-Host "  $($attempt.Name): $($_.Exception.Message)" -ForegroundColor DarkGray
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

    # Update Apps.csv SharedUsersCount/SharedGroupsCount from actual permission data
    # The admin API's sharedGroupsCount is often empty; actual counts are more reliable
    Write-Host "  Updating Apps.csv with accurate shared user/group counts..." -ForegroundColor DarkGray
    try {
        $permsCsv = Import-Csv "$OutputPath/AppPermissions.csv"
        $appsCsvData = Import-Csv "$OutputPath/Apps.csv"
        # Count users and groups per app from permissions
        $permCounts = @{}
        foreach ($perm in $permsCsv) {
            $key = $perm.AppId
            if (-not $permCounts.ContainsKey($key)) {
                $permCounts[$key] = @{ Users = 0; Groups = 0 }
            }
            if ($perm.PrincipalType -eq 'Group') {
                $permCounts[$key].Groups++
            }
            elseif ($perm.PrincipalType -eq 'User') {
                $permCounts[$key].Users++
            }
        }
        # Update the Apps.csv with actual counts
        $updated = 0
        foreach ($app in $appsCsvData) {
            if ($permCounts.ContainsKey($app.AppId)) {
                $app.SharedUsersCount = $permCounts[$app.AppId].Users
                $app.SharedGroupsCount = $permCounts[$app.AppId].Groups
                $updated++
            }
        }
        $appsCsvData | Export-Csv "$OutputPath/Apps.csv" -NoTypeInformation
        Write-Host "  Updated $updated apps with permission-based sharing counts" -ForegroundColor Gray
    }
    catch {
        Write-Host "  Warning: Could not update sharing counts: $($_.Exception.Message)" -ForegroundColor DarkYellow
    }
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
Write-Host "     - Connections.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - AppConnectorRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - FlowActions.FlowKey -> Flows.FlowKey" -ForegroundColor Gray
Write-Host "     - FlowTriggers.FlowKey -> Flows.FlowKey" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.FlowKey -> Flows.FlowKey" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - DlpConnectorRules.PolicyId -> DlpPolicies.PolicyId" -ForegroundColor Gray
Write-Host "     - UsageAnalytics.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host "     - CopilotAgents.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host "     - CopilotComponents.BotId -> CopilotAgents.BotId" -ForegroundColor Gray
Write-Host "     - CopilotComponents.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host ""
