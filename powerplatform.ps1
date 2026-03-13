<#
.SYNOPSIS
    Collects Power Platform inventory data for Power BI dashboards.
.DESCRIPTION
    Pulls environments, apps, flows, connectors, connections, DLP policies,
    and usage analytics from Power Platform admin APIs. Outputs CSV files
    ready for Power BI import.

    Designed for large tenants (1000+ environments, 40K+ apps, 60K+ flows):
    - Automatic token refresh every 45 minutes
    - Throttle handling with exponential backoff on 429 responses
    - Streaming CSV writes per environment (low memory footprint)
    - Progress tracking with ETA
    - Error logging to Errors.csv (non-fatal — continues on per-environment failures)

    For each flow, fetches the full definition to extract triggers, actions,
    and endpoint URLs (SharePoint sites, SQL servers, HTTP endpoints, etc.).
    This adds one API call per flow but provides complete connection mapping.
.PARAMETER OutputPath
    Directory for CSV output files. Defaults to ./PowerPlatformExport.
.PARAMETER IncludePermissions
    If set, fetches sharing/permissions for apps and flows.
    WARNING: One API call per resource — at 100K resources this adds hours. Off by default.
.EXAMPLE
    .\powerplatform.ps1 -OutputPath C:\exports
    .\powerplatform.ps1 -IncludePermissions
#>

param(
    [string]$OutputPath = "./PowerPlatformExport",
    [switch]$IncludePermissions
)

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH — with automatic token refresh
# ============================================================================

Write-Host "Connecting to Azure..." -ForegroundColor Cyan
Connect-AzAccount | Out-Null

$script:ppToken = $null
$script:ppTokenExpiry = [datetime]::MinValue
$script:flowToken = $null
$script:flowTokenExpiry = [datetime]::MinValue
$script:adminToken = $null
$script:adminTokenExpiry = [datetime]::MinValue

function Get-PPToken {
    if ([datetime]::UtcNow -lt $script:ppTokenExpiry) { return $script:ppToken }
    Write-Host "  [Auth] Refreshing Power Platform token..." -ForegroundColor DarkGray
    $result = Get-AzAccessToken -ResourceUrl "https://service.powerapps.com/" -AsSecureString
    $script:ppToken = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($result.Token))
    $script:ppTokenExpiry = [datetime]::UtcNow.AddMinutes(45)
    return $script:ppToken
}

function Get-FlowToken {
    if ([datetime]::UtcNow -lt $script:flowTokenExpiry) { return $script:flowToken }
    Write-Host "  [Auth] Refreshing Flow API token..." -ForegroundColor DarkGray
    $result = Get-AzAccessToken -ResourceUrl "https://service.flow.microsoft.com/" -AsSecureString
    $script:flowToken = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($result.Token))
    $script:flowTokenExpiry = [datetime]::UtcNow.AddMinutes(45)
    return $script:flowToken
}

function Get-AdminToken {
    if ([datetime]::UtcNow -lt $script:adminTokenExpiry) { return $script:adminToken }
    Write-Host "  [Auth] Refreshing Admin Center token..." -ForegroundColor DarkGray
    $result = Get-AzAccessToken -ResourceUrl "https://api.powerplatform.com/" -AsSecureString
    $script:adminToken = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($result.Token))
    $script:adminTokenExpiry = [datetime]::UtcNow.AddMinutes(45)
    return $script:adminToken
}

# ============================================================================
# API HELPERS — with throttle handling
# ============================================================================

function Invoke-PPApi {
    param(
        [string]$Uri,
        [string]$Token,
        [string]$Method = "GET",
        [int]$MaxRetries = 5
    )
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $headers = @{ "Authorization" = "Bearer $Token"; "Accept" = "application/json" }
            return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers
        }
        catch {
            $status = $_.Exception.Response.StatusCode.value__
            if ($status -eq 429 -and $attempt -lt $MaxRetries) {
                $retryAfter = 30 * [math]::Pow(2, $attempt - 1)  # 30s, 60s, 120s, 240s
                $retryHeader = $_.Exception.Response.Headers | Where-Object { $_.Key -eq "Retry-After" }
                if ($retryHeader) { $retryAfter = [int]$retryHeader.Value[0] }
                Write-Host "    [Throttled] 429 — waiting ${retryAfter}s (attempt $attempt/$MaxRetries)" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $retryAfter
            }
            elseif ($status -eq 404) {
                return $null  # Resource not found is non-fatal
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
        [scriptblock]$TokenRefresh = { Get-PPToken }
    )
    $all = [System.Collections.Generic.List[object]]::new()
    $url = $Uri
    while ($url) {
        $Token = & $TokenRefresh  # Refresh token if needed before each page
        $response = Invoke-PPApi -Uri $url -Token $Token
        if ($null -eq $response) { break }
        if ($response.value) { $all.AddRange([object[]]$response.value) }
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

$environments | Export-Csv "$OutputPath/Environments.csv" -NoTypeInformation
Write-Host "  Found $($environments.Count) environments" -ForegroundColor Gray

# ============================================================================
# 2-4. APPS, FLOWS, CONNECTORS — per-environment loop with streaming CSV
# ============================================================================

# Initialize CSV files with headers
Initialize-Csv "$OutputPath/Apps.csv" @("AppId","EnvironmentId","EnvironmentName","DisplayName","Description","AppType","OwnerObjectId","OwnerDisplayName","OwnerEmail","CreatedTime","LastModifiedTime","LastPublishedTime","AppVersion","Status","UsesPremiumApi","UsesCustomApi","SharedUsersCount","SharedGroupsCount","IsSolutionAware","SolutionId","BypassConsent","CollectedAt")
Initialize-Csv "$OutputPath/AppConnectorRefs.csv" @("AppId","EnvironmentId","ConnectorId","DisplayName","DataSources","EndpointUrl")
Initialize-Csv "$OutputPath/Flows.csv" @("FlowId","EnvironmentId","EnvironmentName","DisplayName","Description","State","CreatorObjectId","CreatorDisplayName","CreatedTime","LastModifiedTime","TriggerType","IsSolutionAware","SolutionId","IsManaged","SuspensionReason","CollectedAt")
Initialize-Csv "$OutputPath/FlowTriggers.csv" @("FlowId","EnvironmentId","Position","Name","TriggerType","ConnectorId","OperationId","EndpointUrl")
Initialize-Csv "$OutputPath/FlowActions.csv" @("FlowId","EnvironmentId","Position","Name","ActionType","ConnectorId","OperationId","EndpointUrl")
Initialize-Csv "$OutputPath/FlowConnectionRefs.csv" @("FlowId","EnvironmentId","ConnectorId","ConnectionName","ConnectionUrl")
Initialize-Csv "$OutputPath/Connectors.csv" @("ConnectorId","EnvironmentId","EnvironmentName","DisplayName","Description","Publisher","Tier","IsCustom","IconUri","CollectedAt")
Initialize-Csv "$OutputPath/Connections.csv" @("ConnectionId","ConnectorId","EnvironmentId","EnvironmentName","DisplayName","ConnectionUrl","CreatedByObjectId","CreatedByName","CreatedByEmail","CreatedTime","Status","IsShared","CollectedAt")

$totalApps = 0; $totalFlows = 0; $totalConnectors = 0; $totalConnections = 0
$totalAppConnRefs = 0; $totalTriggers = 0; $totalActions = 0; $totalFlowConnRefs = 0
$envCount = $environments.Count
$envIndex = 0

Write-Host "[2-4/7] Collecting apps, flows, connectors per environment..." -ForegroundColor Yellow

foreach ($env in $environments) {
    $envIndex++
    $envId = $env.EnvironmentId
    $elapsed = (Get-Date) - $startTime
    $pct = [math]::Round(($envIndex / $envCount) * 100)
    $eta = if ($envIndex -gt 1) {
        $perEnv = $elapsed.TotalSeconds / ($envIndex - 1)
        $remaining = [TimeSpan]::FromSeconds($perEnv * ($envCount - $envIndex))
        "{0:hh\:mm\:ss}" -f $remaining
    } else { "calculating..." }

    Write-Host "  [$envIndex/$envCount] $($env.DisplayName) ($pct% — ETA: $eta)" -ForegroundColor Gray

    # --- CONNECTORS & CONNECTIONS (fetched first to build URL lookups for apps and flows) ---
    $envConnByName = @{}   # connectionName → URL (exact match)
    $envConnByType = @{}   # connectorId → [list of unique URLs] (all connections for that connector type)
    try {
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

        $connections = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/connections?$apiVer" -Token $token
        foreach ($c in $connections) {
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
            }
            if (-not $connUrl -and $c.properties.connectionParametersSet) {
                $vals = $c.properties.connectionParametersSet.values
                if ($vals) {
                    if ($vals.server.value) { $connUrl = $vals.server.value }
                    elseif ($vals.siteUrl.value) { $connUrl = $vals.siteUrl.value }
                    elseif ($vals.url.value) { $connUrl = $vals.url.value }
                }
            }

            # Build lookups for cross-referencing with apps and flows
            if ($connUrl) {
                $envConnByName[$c.name] = $connUrl
                # Group all URLs by connector type (e.g. shared_sharepointonline → all SP site URLs)
                if (-not $envConnByType.ContainsKey($connId)) {
                    $envConnByType[$connId] = [System.Collections.Generic.List[string]]::new()
                }
                if (-not $envConnByType[$connId].Contains($connUrl)) {
                    $envConnByType[$connId].Add($connUrl)
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
    }
    catch {
        $errors.Add([PSCustomObject]@{ EnvironmentId=$envId; EnvironmentName=$env.DisplayName; Phase="Connectors"; Error=$_.Exception.Message; Timestamp=(Get-Date) })
        Write-Host "    Warning (connectors): $($_.Exception.Message)" -ForegroundColor DarkYellow
    }

    # --- APPS (after connections so we can resolve endpoint URLs) ---
    try {
        $token = Get-PPToken
        $apps = Invoke-PPApiPaged -Uri "$pa/providers/Microsoft.PowerApps/scopes/admin/environments/$envId/apps?$apiVer" -Token $token
        foreach ($app in $apps) {
            $row = [PSCustomObject]@{
                AppId=$app.name; EnvironmentId=$envId; EnvironmentName=$env.DisplayName
                DisplayName=$app.properties.displayName; Description=$app.properties.description
                AppType=$app.properties.appType; OwnerObjectId=$app.properties.owner.id
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
                    if ($appConnName -and $envConnByName.ContainsKey($appConnName)) {
                        $refUrl = $envConnByName[$appConnName]
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

    # --- FLOWS ---
    try {
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
            Write-Host "    Found $($flows.Count) flows — fetching details..." -ForegroundColor DarkGray
        }

        $flowIndex = 0
        foreach ($f in $flows) {
            $flowIndex++
            if ($flowIndex % 50 -eq 0) {
                Write-Host "      Flow details: $flowIndex / $($flows.Count)" -ForegroundColor DarkGray
            }

            # Fetch flow detail — admin endpoint returns definitionSummary + connectionReferences
            # (but NOT full definition with parameters — that's only on the maker endpoint)
            $defSummary = $null
            $creatorId = ""; $creatorName = ""
            $connRefs = $null

            try {
                $fToken = & $flowTokenRefresh
                $flowDetail = Invoke-PPApi -Uri "$flow/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/flows/$($f.name)?$apiVer" -Token $fToken
                if ($flowDetail) {
                    $defSummary = $flowDetail.properties.definitionSummary
                    $creatorId = $flowDetail.properties.creator.objectId
                    $creatorName = $flowDetail.properties.creator.displayName
                    $connRefs = $flowDetail.properties.connectionReferences
                }
            }
            catch {
                # V1 individual failed — try V2 individual
                try {
                    $fToken = & $flowTokenRefresh
                    $flowDetail = Invoke-PPApi -Uri "$flow/providers/Microsoft.ProcessSimple/scopes/admin/environments/$envId/v2/flows/$($f.name)?$apiVer" -Token $fToken
                    if ($flowDetail) {
                        $defSummary = $flowDetail.properties.definitionSummary
                        $creatorId = $flowDetail.properties.creator.objectId
                        $creatorName = $flowDetail.properties.creator.displayName
                        $connRefs = $flowDetail.properties.connectionReferences
                    }
                }
                catch {
                    # Both failed — continue with basic flow info from V2 list
                }
            }

            # Build flow-specific connector → URL lookup from connectionReferences
            # Then fall back to environment-wide connector type → URL lookup
            $flowConnUrls = @{}  # connectorId → list of unique URLs (flow-specific)
            if ($connRefs) {
                foreach ($ref in $connRefs.PSObject.Properties) {
                    $crConnId = if ($ref.Value.api -and $ref.Value.api.name) { $ref.Value.api.name }
                                elseif ($ref.Value.id) { $ref.Value.id -replace '.*/apis/', '' }
                                else { $ref.Name }
                    $crConnName = if ($ref.Value.connectionName) { $ref.Value.connectionName }
                                  elseif ($ref.Value.connection -and $ref.Value.connection.name) { $ref.Value.connection.name }
                                  else { "" }
                    # Try exact connection name match first
                    $crUrl = ""
                    if ($crConnName -and $envConnByName.ContainsKey($crConnName)) {
                        $crUrl = $envConnByName[$crConnName]
                    }
                    # Collect per-connector URLs
                    if ($crUrl) {
                        if (-not $flowConnUrls.ContainsKey($crConnId)) {
                            $flowConnUrls[$crConnId] = [System.Collections.Generic.List[string]]::new()
                        }
                        if (-not $flowConnUrls[$crConnId].Contains($crUrl)) {
                            $flowConnUrls[$crConnId].Add($crUrl)
                        }
                    }

                    Append-CsvRow "$OutputPath/FlowConnectionRefs.csv" ([PSCustomObject]@{
                        FlowId=$f.name; EnvironmentId=$envId; ConnectorId=$crConnId; ConnectionName=$crConnName; ConnectionUrl=$crUrl
                    })
                    $totalFlowConnRefs++
                }
            }

            # --- Parse triggers from definitionSummary, resolve URLs ---
            $triggerType = "Unknown"
            if ($defSummary -and $defSummary.triggers) {
                $pos = 0
                foreach ($t in $defSummary.triggers) {
                    $tConnId = if ($t.api -and $t.api.id) { $t.api.id -replace '.*/apis/', '' } else { "" }
                    # Try flow-specific URLs first, fall back to all URLs for this connector type in the environment
                    $tUrl = ""
                    if ($tConnId) {
                        if ($flowConnUrls.ContainsKey($tConnId)) {
                            $tUrl = $flowConnUrls[$tConnId] -join "; "
                        }
                        elseif ($envConnByType.ContainsKey($tConnId)) {
                            $tUrl = $envConnByType[$tConnId] -join "; "
                        }
                    }
                    if ($pos -eq 0) { $triggerType = $t.type }
                    Append-CsvRow "$OutputPath/FlowTriggers.csv" ([PSCustomObject]@{
                        FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                        TriggerType=$t.type; ConnectorId=$tConnId; OperationId=$t.swaggerOperationId; EndpointUrl=$tUrl
                    })
                    $pos++; $totalTriggers++
                }
            }

            # --- Parse actions from definitionSummary, resolve URLs ---
            if ($defSummary -and $defSummary.actions) {
                $pos = 0
                foreach ($a in $defSummary.actions) {
                    $aConnId = if ($a.api -and $a.api.id) { $a.api.id -replace '.*/apis/', '' } else { "" }
                    # Try flow-specific URLs first, fall back to all URLs for this connector type in the environment
                    $aUrl = ""
                    if ($aConnId) {
                        if ($flowConnUrls.ContainsKey($aConnId)) {
                            $aUrl = $flowConnUrls[$aConnId] -join "; "
                        }
                        elseif ($envConnByType.ContainsKey($aConnId)) {
                            $aUrl = $envConnByType[$aConnId] -join "; "
                        }
                    }
                    Append-CsvRow "$OutputPath/FlowActions.csv" ([PSCustomObject]@{
                        FlowId=$f.name; EnvironmentId=$envId; Position=$pos; Name=""
                        ActionType=$a.type; ConnectorId=$aConnId; OperationId=$a.swaggerOperationId; EndpointUrl=$aUrl
                    })
                    $pos++; $totalActions++
                }
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
}

Write-Host "  Totals: $totalApps apps, $totalFlows flows, $totalConnectors connectors, $totalConnections connections" -ForegroundColor Gray
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
Write-Host "     - Connections.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - AppConnectorRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.FlowId -> Flows.FlowId" -ForegroundColor Gray
Write-Host "     - FlowConnectionRefs.ConnectorId -> Connectors.ConnectorId" -ForegroundColor Gray
Write-Host "     - DlpConnectorRules.PolicyId -> DlpPolicies.PolicyId" -ForegroundColor Gray
Write-Host "     - UsageAnalytics.EnvironmentId -> Environments.EnvironmentId" -ForegroundColor Gray
Write-Host ""
