<#
.SYNOPSIS
    Fixes DLP policy connector data by fetching from both v1 and v2 API endpoints.
.DESCRIPTION
    The main powerplatform.ps1 script only uses the v1 DLP API endpoint (apiPolicies).
    Microsoft has migrated newer DLP policies to a v2 format that may not return
    connector groups through the v1 endpoint. This script:

    1. Authenticates via Azure (same as powerplatform.ps1)
    2. Fetches DLP policies from BOTH v1 and v2 endpoints
    3. Dumps raw API responses to a diagnostics file for troubleshooting
    4. Parses all known connector group formats:
       - v1: properties.connectorGroups[].connectors[]
       - v1 legacy: properties.definition.apiGroups (hpiGroup/lbiGroup/blockedGroup)
       - v2: properties.connectorGroups[] (may use different classification names)
       - v2: properties.policyConnectorConfigurations (newer v2 structure)
    5. Overwrites DlpPolicies.csv and DlpConnectorRules.csv with corrected data

    Run this AFTER powerplatform.ps1 to patch just the DLP data.
.PARAMETER OutputPath
    Directory containing the CSV files from powerplatform.ps1. The script will
    overwrite DlpPolicies.csv and DlpConnectorRules.csv in this folder.
.PARAMETER UseDeviceCode
    Use device code authentication instead of interactive browser login.
.PARAMETER DiagnosticsOnly
    Only dump raw API responses without overwriting CSVs. Use this to inspect
    what the API is actually returning.
.EXAMPLE
    .\Fix-DlpData.ps1 -OutputPath .\PowerPlatformExport
    .\Fix-DlpData.ps1 -OutputPath .\PowerPlatformExport -DiagnosticsOnly
    .\Fix-DlpData.ps1 -OutputPath .\PowerPlatformExport -UseDeviceCode
#>

param(
    [Parameter(Mandatory)]
    [string]$OutputPath,
    [switch]$UseDeviceCode,
    [switch]$DiagnosticsOnly
)

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH
# ============================================================================

Write-Host "`n=== DLP Policy Data Fix ===" -ForegroundColor Cyan
Write-Host "Output path: $OutputPath`n" -ForegroundColor Gray

if (-not (Test-Path $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
}

Write-Host "Connecting to Azure..." -ForegroundColor Cyan
$connectArgs = @{}
if ($UseDeviceCode) { $connectArgs['UseDeviceAuthentication'] = $true }
try {
    Connect-AzAccount @connectArgs | Out-Null
}
catch {
    Write-Host "  Auth failed ($($_.Exception.Message)), retrying..." -ForegroundColor DarkYellow
    Connect-AzAccount @connectArgs | Out-Null
}

function Get-TokenString {
    param([securestring]$SecureToken)
    [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureToken))
}

function Get-FreshToken {
    param([string]$ResourceUrl)
    $result = Get-AzAccessToken -ResourceUrl $ResourceUrl -AsSecureString
    return Get-TokenString $result.Token
}

function Invoke-ApiCall {
    param([string]$Uri, [string]$Token, [int]$MaxRetries = 3)
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $headers = @{ "Authorization" = "Bearer $Token"; "Accept" = "application/json" }
            return Invoke-RestMethod -Uri $Uri -Method GET -Headers $headers -TimeoutSec 60
        }
        catch {
            $status = $_.Exception.Response.StatusCode.value__
            if ($status -eq 401 -and $attempt -lt $MaxRetries) {
                Write-Host "    [Auth] 401 — refreshing token (attempt $attempt)" -ForegroundColor Yellow
                $Token = Get-FreshToken "https://service.powerapps.com/"
            }
            elseif ($status -eq 429 -and $attempt -lt $MaxRetries) {
                $wait = 30 * $attempt
                Write-Host "    [Throttled] 429 — waiting ${wait}s" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $wait
            }
            elseif ($status -eq 403 -or $status -eq 404) {
                Write-Host "    [$status] $Uri" -ForegroundColor DarkGray
                return $null
            }
            elseif ($attempt -ge $MaxRetries) {
                Write-Host "    [Error] Failed after $MaxRetries attempts: $($_.Exception.Message)" -ForegroundColor Red
                return $null
            }
            else {
                Start-Sleep -Seconds (5 * $attempt)
            }
        }
    }
    return $null
}

function Invoke-ApiPaged {
    param([string]$Uri, [string]$Token, [int]$MaxPages = 100)
    $all = [System.Collections.Generic.List[object]]::new()
    $url = $Uri
    $page = 0
    while ($url -and $page -lt $MaxPages) {
        $page++
        $response = Invoke-ApiCall -Uri $url -Token $Token
        if ($null -eq $response) { break }
        if ($response.value -and $response.value.Count -gt 0) {
            $all.AddRange([object[]]$response.value)
        }
        else { break }
        $url = if ($response.nextLink) { $response.nextLink }
               elseif ($response.'@odata.nextLink') { $response.'@odata.nextLink' }
               else { $null }
    }
    return $all
}

# ============================================================================
# FETCH DLP POLICIES FROM ALL KNOWN ENDPOINTS
# ============================================================================

$bap = "https://api.bap.microsoft.com"
$timestamp = (Get-Date).ToUniversalTime().ToString("o")
$diagFile = "$OutputPath/_dlp_diagnostics.json"

$allPoliciesRaw = [System.Collections.Generic.List[object]]::new()
$policySourceMap = @{}  # policyId -> which endpoint found it

# Try multiple token/endpoint combinations
$endpoints = @(
    @{
        Name = "v1 (PP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies?api-version=2016-11-01"
        Resource = "https://service.powerapps.com/"
    }
    @{
        Name = "v1 (BAP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies?api-version=2016-11-01"
        Resource = "https://api.bap.microsoft.com/"
    }
    @{
        Name = "v2 (PP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/v2/apiPolicies?api-version=2016-11-01"
        Resource = "https://service.powerapps.com/"
    }
    @{
        Name = "v2 (BAP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/v2/apiPolicies?api-version=2016-11-01"
        Resource = "https://api.bap.microsoft.com/"
    }
    @{
        Name = "v2 2021-04-01 (PP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/v2/apiPolicies?api-version=2021-04-01"
        Resource = "https://service.powerapps.com/"
    }
    @{
        Name = "v2 2024-01-01 (PP token)"
        Uri  = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/v2/apiPolicies?api-version=2024-01-01"
        Resource = "https://service.powerapps.com/"
    }
)

Write-Host "`n[1/3] Probing DLP API endpoints..." -ForegroundColor Yellow

foreach ($ep in $endpoints) {
    Write-Host "  Trying: $($ep.Name)..." -ForegroundColor Gray -NoNewline
    try {
        $token = Get-FreshToken $ep.Resource
        $policies = Invoke-ApiPaged -Uri $ep.Uri -Token $token
        $newCount = 0
        foreach ($p in $policies) {
            $polId = $p.name
            if (-not $policySourceMap.ContainsKey($polId)) {
                $policySourceMap[$polId] = $ep.Name
                $allPoliciesRaw.Add($p)
                $newCount++
            }
        }
        Write-Host " $($policies.Count) policies ($newCount new)" -ForegroundColor $(if ($newCount -gt 0) { "Green" } else { "DarkGray" })
    }
    catch {
        Write-Host " Failed: $($_.Exception.Message)" -ForegroundColor DarkYellow
    }
}

Write-Host "`n  Total unique policies found: $($allPoliciesRaw.Count)" -ForegroundColor Cyan

# ============================================================================
# DIAGNOSTICS — dump raw response structure
# ============================================================================

Write-Host "`n[2/3] Analyzing policy structures..." -ForegroundColor Yellow

$diagnostics = [System.Collections.Generic.List[object]]::new()
$allDlpPolicies = [System.Collections.Generic.List[object]]::new()
$allDlpConnectorRules = [System.Collections.Generic.List[object]]::new()

foreach ($d in $allPoliciesRaw) {
    $policyId = $d.name
    $policyName = $d.properties.displayName
    $source = $policySourceMap[$policyId]

    # Detect which format this policy uses
    $hasConnectorGroups = $null -ne $d.properties.connectorGroups -and $d.properties.connectorGroups.Count -gt 0
    $hasApiGroups = $null -ne $d.properties.definition -and $null -ne $d.properties.definition.apiGroups
    $hasConnectorConfigs = $null -ne $d.properties.policyConnectorConfigurations
    $hasCustomConnectorConfigs = $null -ne $d.properties.customConnectorConfigurations

    # Enumerate all property names on the policy for diagnostics
    $topProps = @($d.PSObject.Properties.Name)
    $propsProps = @()
    if ($d.properties) { $propsProps = @($d.properties.PSObject.Properties.Name) }

    $connectorRulesFromPolicy = 0

    # --- Format 1: connectorGroups (most common for v1 and v2) ---
    if ($hasConnectorGroups) {
        foreach ($group in $d.properties.connectorGroups) {
            $classification = $group.classification
            # Normalize classification names (v2 may use Confidential/General instead of Business/NonBusiness)
            $normalizedClass = switch ($classification) {
                "Confidential" { "Business" }
                "General"      { "NonBusiness" }
                default        { $classification }
            }
            $connectors = $group.connectors
            if (-not $connectors) { continue }
            foreach ($conn in $connectors) {
                $connId = $conn.id -replace '.*/apis/', ''
                $connName = if ($conn.name) { $conn.name }
                            elseif ($conn.properties -and $conn.properties.displayName) { $conn.properties.displayName }
                            else { $connId }
                $allDlpConnectorRules.Add([PSCustomObject]@{
                    PolicyId       = $policyId
                    PolicyName     = $policyName
                    ConnectorId    = $connId
                    ConnectorName  = $connName
                    Classification = $normalizedClass
                })
                $connectorRulesFromPolicy++
            }
        }
    }

    # --- Format 2: definition.apiGroups (legacy v1) ---
    if ($hasApiGroups -and $connectorRulesFromPolicy -eq 0) {
        foreach ($group in $d.properties.definition.apiGroups.PSObject.Properties) {
            $cls = switch ($group.Name) {
                "hbiGroup"     { "Business" }    # High Business Impact
                "hpiGroup"     { "Business" }    # High Priority Impact (alternate name)
                "lbiGroup"     { "NonBusiness" } # Low Business Impact
                "mbiGroup"     { "NonBusiness" } # Medium Business Impact (v2 variant)
                "blockedGroup" { "Blocked" }
                default        { $group.Name }
            }
            $apis = $group.Value.apis
            if (-not $apis) { continue }
            foreach ($conn in $apis) {
                $connId = $conn.id -replace '.*/apis/', ''
                $connName = if ($conn.name) { $conn.name } else { $connId }
                $allDlpConnectorRules.Add([PSCustomObject]@{
                    PolicyId       = $policyId
                    PolicyName     = $policyName
                    ConnectorId    = $connId
                    ConnectorName  = $connName
                    Classification = $cls
                })
                $connectorRulesFromPolicy++
            }
        }
    }

    # --- Format 3: Try to find connectors anywhere in the policy object ---
    if ($connectorRulesFromPolicy -eq 0) {
        # Last resort: walk the properties looking for any array that contains connector-like objects
        $searchProps = @("businessDataGroup", "nonBusinessDataGroup", "blockedGroup",
                         "confidentialGroup", "generalGroup")
        foreach ($propName in $searchProps) {
            $propVal = $null
            if ($d.properties.PSObject.Properties.Name -contains $propName) {
                $propVal = $d.properties.$propName
            }
            elseif ($d.properties.definition -and $d.properties.definition.PSObject.Properties.Name -contains $propName) {
                $propVal = $d.properties.definition.$propName
            }
            if ($propVal -and $propVal.Count -gt 0) {
                $cls = switch -Wildcard ($propName) {
                    "*business*"     { "Business" }
                    "*nonBusiness*"  { "NonBusiness" }
                    "*blocked*"      { "Blocked" }
                    "*confidential*" { "Business" }
                    "*general*"      { "NonBusiness" }
                    default          { $propName }
                }
                foreach ($conn in $propVal) {
                    $connId = if ($conn.id) { $conn.id -replace '.*/apis/', '' } elseif ($conn.connectorId) { $conn.connectorId } else { "unknown" }
                    $connName = if ($conn.name) { $conn.name } elseif ($conn.displayName) { $conn.displayName } else { $connId }
                    $allDlpConnectorRules.Add([PSCustomObject]@{
                        PolicyId       = $policyId
                        PolicyName     = $policyName
                        ConnectorId    = $connId
                        ConnectorName  = $connName
                        Classification = $cls
                    })
                    $connectorRulesFromPolicy++
                }
            }
        }
    }

    $formatDetected = if ($hasConnectorGroups) { "connectorGroups" }
                      elseif ($hasApiGroups) { "definition.apiGroups" }
                      else { "unknown" }

    $diagEntry = [ordered]@{
        PolicyId              = $policyId
        PolicyName            = $policyName
        Source                = $source
        Format                = $formatDetected
        ConnectorRulesFound   = $connectorRulesFromPolicy
        HasConnectorGroups    = $hasConnectorGroups
        HasApiGroups          = $hasApiGroups
        HasConnectorConfigs   = $hasConnectorConfigs
        HasCustomConnConfigs  = $hasCustomConnectorConfigs
        TopLevelProperties    = ($topProps -join ", ")
        PropertiesProperties  = ($propsProps -join ", ")
    }

    # If zero connectors found, dump the full raw policy for debugging
    if ($connectorRulesFromPolicy -eq 0) {
        $diagEntry.RawPolicyJson = ($d | ConvertTo-Json -Depth 10)
        Write-Host "  WARNING: 0 connectors found for '$policyName' (format: $formatDetected)" -ForegroundColor Red
    }
    else {
        Write-Host "  OK: $connectorRulesFromPolicy connectors for '$policyName' (format: $formatDetected, source: $source)" -ForegroundColor Green
    }

    $diagnostics.Add([PSCustomObject]$diagEntry)

    # Build the policy row
    $isEnabled = if ($d.properties.PSObject.Properties.Name -contains 'isDisabled') { -not $d.properties.isDisabled } else { $true }
    $policyType = $d.properties.type
    $envScope = if ($d.properties.environmentFilterType) { $d.properties.environmentFilterType }
                elseif ($d.properties.environments) { "IncludeEnvironments" }
                else { "AllEnvironments" }

    $allDlpPolicies.Add([PSCustomObject]@{
        PolicyId         = $policyId
        DisplayName      = $policyName
        Description      = $d.properties.description
        IsEnabled        = $isEnabled
        PolicyType       = $policyType
        EnvironmentScope = $envScope
        CreatedTime      = $d.properties.createdTime
        LastModifiedTime = $d.properties.lastModifiedTime
        CollectedAt      = $timestamp
    })
}

# ============================================================================
# WRITE DIAGNOSTICS
# ============================================================================

$diagnostics | ConvertTo-Json -Depth 15 | Set-Content -Path $diagFile -Encoding UTF8
Write-Host "`n  Diagnostics written to: $diagFile" -ForegroundColor Gray

# ============================================================================
# SUMMARY & EXPORT
# ============================================================================

Write-Host "`n[3/3] Results" -ForegroundColor Yellow

$rulesByClass = $allDlpConnectorRules | Group-Object Classification
Write-Host "  Policies:  $($allDlpPolicies.Count)" -ForegroundColor Cyan
Write-Host "  Connector Rules: $($allDlpConnectorRules.Count)" -ForegroundColor Cyan
foreach ($g in $rulesByClass) {
    Write-Host "    $($g.Name): $($g.Count)" -ForegroundColor Gray
}

$zeroPolicies = $diagnostics | Where-Object { $_.ConnectorRulesFound -eq 0 }
if ($zeroPolicies) {
    Write-Host "`n  WARNING: $($zeroPolicies.Count) policies returned 0 connector rules:" -ForegroundColor Red
    foreach ($zp in $zeroPolicies) {
        Write-Host "    - $($zp.PolicyName) (format: $($zp.Format), source: $($zp.Source))" -ForegroundColor DarkYellow
    }
    Write-Host "  Check $diagFile for full raw API responses" -ForegroundColor DarkYellow
}

if ($DiagnosticsOnly) {
    Write-Host "`n  [DiagnosticsOnly] Skipping CSV overwrite. Review $diagFile" -ForegroundColor Yellow
}
else {
    $dlpCsv = "$OutputPath/DlpPolicies.csv"
    $rulesCsv = "$OutputPath/DlpConnectorRules.csv"

    # Back up existing files
    if (Test-Path $dlpCsv) {
        Copy-Item $dlpCsv "$OutputPath/DlpPolicies.csv.bak" -Force
        Write-Host "  Backed up existing DlpPolicies.csv -> DlpPolicies.csv.bak" -ForegroundColor DarkGray
    }
    if (Test-Path $rulesCsv) {
        Copy-Item $rulesCsv "$OutputPath/DlpConnectorRules.csv.bak" -Force
        Write-Host "  Backed up existing DlpConnectorRules.csv -> DlpConnectorRules.csv.bak" -ForegroundColor DarkGray
    }

    $allDlpPolicies | Export-Csv $dlpCsv -NoTypeInformation
    $allDlpConnectorRules | Export-Csv $rulesCsv -NoTypeInformation

    Write-Host "`n  Wrote $($allDlpPolicies.Count) policies to $dlpCsv" -ForegroundColor Green
    Write-Host "  Wrote $($allDlpConnectorRules.Count) connector rules to $rulesCsv" -ForegroundColor Green
}

Write-Host "`nDone.`n" -ForegroundColor Cyan
