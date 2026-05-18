<#
.SYNOPSIS
    Collects Power Platform DLP policies + environments and pre-computes
    cleanup analysis (overlaps, duplicates, coverage gaps).
.DESCRIPTION
    Tenant-level DLP audit. Pulls every DLP (apiPolicies) policy and the full
    environment list, resolves which policy applies to which environment, and
    writes analysis CSVs aimed at a DLP cleanup job:

      Environments.csv               One row per environment.
      DlpPolicies.csv                One row per policy (enriched: scope,
                                     default classification, connector counts,
                                     created/modified by).
      DlpConnectorRules.csv          Policy x connector x data group.
      DlpPolicyEnvironments.csv      Policy -> environment mapping (expanded,
                                     resolved to environment names).
      EnvironmentDlpCoverage.csv     Environment -> policies, with a coverage
                                     status flag (GAP / Covered / OVERLAP).
      DlpPolicyOverlap.csv           Pairs of policies whose effective
                                     environment sets intersect.
      DlpDuplicatePolicies.csv       Policy pairs by rule-set similarity
                                     (exact duplicates + Jaccard near-dupes).
      DlpConnectorInconsistency.csv  Connectors blocked in some policies but
                                     allowed in others (block gaps).

    Classification mapping (Power Platform DLP API -> admin UI group):
      Confidential = Business        General = Non-Business        Blocked = Blocked
.PARAMETER OutputPath
    Directory for CSV output. Defaults to ./PowerPlatformDlpExport.
.PARAMETER UseDeviceCode
    Use device code auth instead of interactive browser login.
.PARAMETER SimilarityThreshold
    Minimum Jaccard similarity (0-1) for a policy pair to appear in
    DlpDuplicatePolicies.csv. Default 0.6. Exact duplicates always included.
.PARAMETER BlockedConnectorBaseline
    Optional path to a text/CSV file listing connector IDs that SHOULD be
    blocked (one per line, e.g. shared_twitter). When supplied, the script
    flags policies/environments where a baseline connector is not Blocked.
.EXAMPLE
    .\powerplatform-dlp.ps1
    .\powerplatform-dlp.ps1 -OutputPath C:\exports\dlp -SimilarityThreshold 0.5
    .\powerplatform-dlp.ps1 -BlockedConnectorBaseline .\should-be-blocked.txt
#>

param(
    [string]$OutputPath = "./PowerPlatformDlpExport",
    [switch]$UseDeviceCode,
    [double]$SimilarityThreshold = 0.6,
    [string]$BlockedConnectorBaseline = ""
)

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH (faithful to powerplatform.ps1 — known-working path in this tenant)
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
        Write-Host "  [Auth] Session expired - re-authenticating..." -ForegroundColor Yellow
        Connect-AzAccount @script:connectArgs | Out-Null
        $result = Get-AzAccessToken -ResourceUrl "https://service.powerapps.com/" -AsSecureString
    }
    $script:ppToken = Get-TokenString $result.Token
    $script:ppTokenExpiry = [datetime]::UtcNow.AddMinutes(20)
    return $script:ppToken
}

function Reset-AllTokens { $script:ppTokenExpiry = [datetime]::MinValue }

# ============================================================================
# API HELPERS — throttle / 401 handling (from powerplatform.ps1)
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
            $status = 0; try { $status = $_.Exception.Response.StatusCode.value__ } catch {}
            if ($status -eq 401 -and $attempt -le 2) {
                Write-Host "    [Auth] 401 - refreshing token (attempt $attempt/2)" -ForegroundColor Yellow
                Reset-AllTokens
                if ($TokenRefresh) { $Token = & $TokenRefresh }
            }
            elseif ($status -eq 401) { return $null }
            elseif ($status -eq 429 -and $attempt -lt $MaxRetries) {
                $retryAfter = 30 * [math]::Pow(2, $attempt - 1)
                $retryHeader = $_.Exception.Response.Headers | Where-Object { $_.Key -eq "Retry-After" }
                if ($retryHeader) { $retryAfter = [int]$retryHeader.Value[0] }
                Write-Host "    [Throttled] 429 - waiting ${retryAfter}s (attempt $attempt/$MaxRetries)" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $retryAfter
            }
            elseif ($status -eq 403 -or $status -eq 404) { return $null }
            elseif ($attempt -lt $MaxRetries) {
                $wait = 5 * $attempt
                Write-Host "    [Retry] Error attempt $attempt/$MaxRetries - waiting ${wait}s ($($_.Exception.Message))" -ForegroundColor DarkYellow
                Start-Sleep -Seconds $wait
            }
            else { throw }
        }
    }
    throw "Max retries ($MaxRetries) exceeded for $Uri"
}

function Invoke-PPApiPaged {
    param([string]$Uri, [string]$Token, [scriptblock]$TokenRefresh = { Get-PPToken }, [int]$MaxPages = 500)
    $all = [System.Collections.Generic.List[object]]::new()
    $url = $Uri
    $page = 0
    $seenUrls = [System.Collections.Generic.HashSet[string]]::new()
    while ($url) {
        $page++
        if ($page -gt $MaxPages) { break }
        if (-not $seenUrls.Add($url)) { break }
        $Token = & $TokenRefresh
        $response = Invoke-PPApi -Uri $url -Token $Token -TokenRefresh $TokenRefresh
        if ($null -eq $response) { break }
        if ($response.value -and $response.value.Count -gt 0) {
            $all.AddRange([object[]]$response.value)
        } else { break }
        $url = if ($response.nextLink) { $response.nextLink }
               elseif ($response.'@odata.nextLink') { $response.'@odata.nextLink' }
               else { $null }
    }
    return $all
}

# ============================================================================
# HELPERS — classification + connector id normalization
# ============================================================================

function Get-DataGroup {
    # Map raw DLP API classification -> admin-center group name
    param([string]$Raw)
    switch -Regex ($Raw) {
        '^(Confidential|hbi|hpi|hbiGroup|hpiGroup)$' { "Business"; break }
        '^(General|lbi|lbiGroup)$'                   { "Non-Business"; break }
        '^(Blocked|blocked|blockedGroup)$'           { "Blocked"; break }
        default                                      { if ($Raw) { $Raw } else { "(unknown)" } }
    }
}

function Get-ConnectorId {
    param([string]$RawId)
    if (-not $RawId) { return "" }
    ($RawId -replace '.*/apis/', '')
}

# ============================================================================
# SETUP
# ============================================================================

New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
$bap = "https://api.bap.microsoft.com"
$apiVer = "api-version=2016-11-01"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$startTime = Get-Date

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Power Platform DLP Audit" -ForegroundColor Green
Write-Host " Output: $OutputPath" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# ============================================================================
# 1. ENVIRONMENTS
# ============================================================================

Write-Host "[1/3] Collecting environments..." -ForegroundColor Yellow
$token = Get-PPToken
$envUri = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?$apiVer"
$envs = Invoke-PPApiPaged -Uri $envUri -Token $token

$environments = $envs | ForEach-Object {
    [PSCustomObject]@{
        EnvironmentId      = $_.name
        DisplayName        = $_.properties.displayName
        EnvironmentType    = $_.properties.environmentSku
        Region             = $_.properties.azureRegion
        State              = $_.properties.states.management.id
        IsDefault          = [bool]$_.properties.isDefault
        IsDataverseEnabled = [bool]$_.properties.linkedEnvironmentMetadata
        CreatedTime        = $_.properties.createdTime
        LastModifiedTime   = $_.properties.lastModifiedTime
        CollectedAt        = $timestamp
    }
}
$environments | Export-Csv "$OutputPath/Environments.csv" -NoTypeInformation
$allEnvIds = [System.Collections.Generic.HashSet[string]]::new(
    [string[]]@($environments | ForEach-Object { $_.EnvironmentId }))
$envNameById = @{}
foreach ($e in $environments) { $envNameById[$e.EnvironmentId] = $e.DisplayName }
Write-Host "  Found $($environments.Count) environments" -ForegroundColor Gray

# ============================================================================
# 2. DLP POLICIES
# ============================================================================

Write-Host "[2/3] Collecting DLP policies..." -ForegroundColor Yellow
$token = Get-PPToken
$dlpUri = "$bap/providers/Microsoft.BusinessAppPlatform/scopes/admin/apiPolicies?$apiVer"
$dlps = Invoke-PPApiPaged -Uri $dlpUri -Token $token

$allDlpPolicies        = [System.Collections.Generic.List[object]]::new()
$allDlpConnectorRules  = [System.Collections.Generic.List[object]]::new()
$allDlpPolicyEnvs      = [System.Collections.Generic.List[object]]::new()

# In-memory model for the analysis pass:
#   $policyModel[policyId] = @{ Name; Scope; EnvSet(HashSet); IsDefault;
#                               Connectors(@{connId=dataGroup}) }
$policyModel = @{}

foreach ($d in $dlps) {
    $polId   = $d.name
    $pname = $d.properties.displayName
    $scope = $d.properties.environmentType
    if (-not $scope) { $scope = $d.properties.environmentFilterType }
    if (-not $scope) { $scope = "AllEnvironments" }
    $defaultCls = $d.properties.defaultConnectorsClassification
    $defaultGrp = Get-DataGroup $defaultCls

    # ---- explicit environment list on the policy ----
    $scopedEnvIds = @()
    if ($d.properties.environments) {
        $scopedEnvIds = @($d.properties.environments | ForEach-Object {
            if ($_.name) { $_.name } elseif ($_.id) { Get-ConnectorId $_.id } elseif ($_.environmentName) { $_.environmentName }
        } | Where-Object { $_ })
    }

    # ---- effective environment set (what the policy actually governs) ----
    $effective = [System.Collections.Generic.HashSet[string]]::new()
    switch -Regex ($scope) {
        'Only'   { foreach ($x in $scopedEnvIds) { [void]$effective.Add($x) } }
        'Except' { foreach ($x in $allEnvIds) { if ($scopedEnvIds -notcontains $x) { [void]$effective.Add($x) } } }
        default  { foreach ($x in $allEnvIds) { [void]$effective.Add($x) } }   # AllEnvironments
    }
    $isDefault = ($scope -match 'All|Except')

    # ---- connector rules ----
    $connMap = @{}
    if ($d.properties.connectorGroups) {
        foreach ($group in $d.properties.connectorGroups) {
            $grp = Get-DataGroup $group.classification
            foreach ($conn in $group.connectors) {
                $cid = Get-ConnectorId $conn.id
                if (-not $cid) { continue }
                $connMap[$cid] = $grp
                $allDlpConnectorRules.Add([PSCustomObject]@{
                    PolicyId=$polId; PolicyName=$pname; ConnectorId=$cid
                    ConnectorName=$conn.name; Classification=$group.classification; DataGroup=$grp
                })
            }
        }
    }
    elseif ($d.properties.definition -and $d.properties.definition.apiGroups) {
        foreach ($group in $d.properties.definition.apiGroups.PSObject.Properties) {
            $grp = Get-DataGroup $group.Name
            foreach ($conn in $group.Value.apis) {
                $cid = Get-ConnectorId $conn.id
                if (-not $cid) { continue }
                $connMap[$cid] = $grp
                $allDlpConnectorRules.Add([PSCustomObject]@{
                    PolicyId=$polId; PolicyName=$pname; ConnectorId=$cid
                    ConnectorName=$conn.name; Classification=$group.Name; DataGroup=$grp
                })
            }
        }
    }

    $bizCount   = ($connMap.Values | Where-Object { $_ -eq 'Business' }).Count
    $nbizCount  = ($connMap.Values | Where-Object { $_ -eq 'Non-Business' }).Count
    $blkCount   = ($connMap.Values | Where-Object { $_ -eq 'Blocked' }).Count

    $allDlpPolicies.Add([PSCustomObject]@{
        PolicyId=$polId; DisplayName=$pname; Description=$d.properties.description
        IsEnabled=(-not $d.properties.isDisabled); PolicyType=$d.properties.type
        EnvironmentScope=$scope; ScopedEnvironmentCount=$scopedEnvIds.Count
        EffectiveEnvironmentCount=$effective.Count; IsTenantWideDefault=$isDefault
        DefaultConnectorClassification=$defaultGrp
        BusinessConnectors=$bizCount; NonBusinessConnectors=$nbizCount
        BlockedConnectors=$blkCount; TotalClassifiedConnectors=$connMap.Count
        CreatedBy=$d.properties.createdBy.displayName; CreatedTime=$d.properties.createdTime
        LastModifiedBy=$d.properties.lastModifiedBy.displayName
        LastModifiedTime=$d.properties.lastModifiedTime; CollectedAt=$timestamp
    })

    # ---- policy -> environment mapping (expanded + resolved) ----
    if ($scope -match 'Only|Except') {
        foreach ($eid in $effective) {
            $allDlpPolicyEnvs.Add([PSCustomObject]@{
                PolicyId=$polId; PolicyName=$pname; EnvironmentScope=$scope
                EnvironmentId=$eid; EnvironmentName=$envNameById[$eid]; CollectedAt=$timestamp
            })
        }
    } else {
        $allDlpPolicyEnvs.Add([PSCustomObject]@{
            PolicyId=$polId; PolicyName=$pname; EnvironmentScope=$scope
            EnvironmentId="(ALL ENVIRONMENTS)"; EnvironmentName="(tenant-wide default)"; CollectedAt=$timestamp
        })
    }

    $policyModel[$polId] = @{
        Name=$pname; Scope=$scope; EnvSet=$effective; IsDefault=$isDefault
        Connectors=$connMap; Enabled=(-not $d.properties.isDisabled)
        DefaultGroup=$defaultGrp
    }
}

$allDlpPolicies       | Export-Csv "$OutputPath/DlpPolicies.csv" -NoTypeInformation
$allDlpConnectorRules | Export-Csv "$OutputPath/DlpConnectorRules.csv" -NoTypeInformation
$allDlpPolicyEnvs     | Export-Csv "$OutputPath/DlpPolicyEnvironments.csv" -NoTypeInformation
Write-Host "  Found $($allDlpPolicies.Count) policies, $($allDlpConnectorRules.Count) connector rules" -ForegroundColor Gray

# ============================================================================
# 3. CLEANUP ANALYSIS
# ============================================================================

Write-Host "[3/3] Computing overlap / duplicate / gap analysis..." -ForegroundColor Yellow
$policyIds = @($policyModel.Keys)

# ---- 3a. Environment coverage (what policy is tied to what environment) ----
$coverage = [System.Collections.Generic.List[object]]::new()
foreach ($e in $environments) {
    $eid = $e.EnvironmentId
    $applied   = @($policyIds | Where-Object { $policyModel[$_].EnvSet.Contains($eid) })
    $scopedP   = @($applied | Where-Object { -not $policyModel[$_].IsDefault })
    $defaultP  = @($applied | Where-Object {      $policyModel[$_].IsDefault })

    $status =
        if     ($applied.Count -eq 0)   { "GAP - No DLP Policy" }
        elseif ($scopedP.Count -gt 1)   { "OVERLAP - Multiple scoped policies" }
        elseif ($scopedP.Count -eq 1)   { "Covered (scoped policy)" }
        elseif ($defaultP.Count -gt 1)  { "OVERLAP - Multiple tenant-default policies" }
        else                            { "Covered (tenant default only)" }

    $coverage.Add([PSCustomObject]@{
        EnvironmentId=$eid; EnvironmentName=$e.DisplayName; EnvironmentType=$e.EnvironmentType
        IsDataverseEnabled=$e.IsDataverseEnabled
        AppliedPolicyCount=$applied.Count
        ScopedPolicyCount=$scopedP.Count
        TenantDefaultPolicyCount=$defaultP.Count
        CoverageStatus=$status
        AppliedPolicyIds=($applied -join "; ")
        AppliedPolicyNames=(($applied | ForEach-Object { $policyModel[$_].Name }) -join "; ")
        CollectedAt=$timestamp
    })
}
$coverage | Export-Csv "$OutputPath/EnvironmentDlpCoverage.csv" -NoTypeInformation

# ---- 3b. Policy overlap (effective env sets intersect) ----
$overlap = [System.Collections.Generic.List[object]]::new()
for ($i = 0; $i -lt $policyIds.Count; $i++) {
    for ($j = $i + 1; $j -lt $policyIds.Count; $j++) {
        $a = $policyModel[$policyIds[$i]]; $b = $policyModel[$policyIds[$j]]
        $shared = @($a.EnvSet | Where-Object { $b.EnvSet.Contains($_) })
        if ($shared.Count -gt 0) {
            $kind = if ($a.IsDefault -and $b.IsDefault) {
                        "Both tenant-wide default (only one allowed)"
                    } elseif ($a.IsDefault -or $b.IsDefault) {
                        "Default vs scoped (specific policy wins - usually OK)"
                    } else {
                        "Two scoped policies share environments (conflict)"
                    }
            $overlap.Add([PSCustomObject]@{
                PolicyA_Id=$policyIds[$i]; PolicyA_Name=$a.Name; PolicyA_Scope=$a.Scope
                PolicyB_Id=$policyIds[$j]; PolicyB_Name=$b.Name; PolicyB_Scope=$b.Scope
                SharedEnvironmentCount=$shared.Count
                OverlapKind=$kind
                SharedEnvironmentSample=(($shared | Select-Object -First 10 | ForEach-Object { $envNameById[$_] }) -join "; ")
                CollectedAt=$timestamp
            })
        }
    }
}
$overlap | Export-Csv "$OutputPath/DlpPolicyOverlap.csv" -NoTypeInformation

# ---- 3c. Duplicate / near-duplicate policies (rule-set Jaccard) ----
function Get-RuleSet { param($cm) [System.Collections.Generic.HashSet[string]]::new(
    [string[]]@($cm.Keys | ForEach-Object { "$_=$($cm[$_])" })) }

$ruleSets = @{}
foreach ($polId in $policyIds) { $ruleSets[$polId] = Get-RuleSet $policyModel[$polId].Connectors }

$dupes = [System.Collections.Generic.List[object]]::new()
for ($i = 0; $i -lt $policyIds.Count; $i++) {
    for ($j = $i + 1; $j -lt $policyIds.Count; $j++) {
        $sa = $ruleSets[$policyIds[$i]]; $sb = $ruleSets[$policyIds[$j]]
        if ($sa.Count -eq 0 -and $sb.Count -eq 0) { continue }
        $inter = @($sa | Where-Object { $sb.Contains($_) }).Count
        $union = $sa.Count + $sb.Count - $inter
        $jac = if ($union -eq 0) { 0 } else { [math]::Round($inter / $union, 3) }
        $exact = ($sa.Count -eq $sb.Count -and $inter -eq $sa.Count -and $sa.Count -gt 0)
        if ($exact -or $jac -ge $SimilarityThreshold) {
            $dupes.Add([PSCustomObject]@{
                PolicyA_Id=$policyIds[$i]; PolicyA_Name=$policyModel[$policyIds[$i]].Name
                PolicyB_Id=$policyIds[$j]; PolicyB_Name=$policyModel[$policyIds[$j]].Name
                SharedConnectorRules=$inter; TotalDistinctRules=$union
                JaccardSimilarity=$jac
                IsExactDuplicate=$exact
                CollectedAt=$timestamp
            })
        }
    }
}
$dupes | Sort-Object -Property JaccardSimilarity -Descending |
    Export-Csv "$OutputPath/DlpDuplicatePolicies.csv" -NoTypeInformation

# ---- 3d. Connector inconsistency (blocked somewhere, allowed elsewhere) ----
$connAll = @{}   # connId -> @{ Name; Groups=@{group=[policyNames]} }
foreach ($r in $allDlpConnectorRules) {
    if (-not $connAll.ContainsKey($r.ConnectorId)) {
        $connAll[$r.ConnectorId] = @{ Name=$r.ConnectorName; Groups=@{} }
    }
    if (-not $connAll[$r.ConnectorId].Groups.ContainsKey($r.DataGroup)) {
        $connAll[$r.ConnectorId].Groups[$r.DataGroup] = [System.Collections.Generic.List[string]]::new()
    }
    $connAll[$r.ConnectorId].Groups[$r.DataGroup].Add($r.PolicyName)
}
$inconsistency = [System.Collections.Generic.List[object]]::new()
foreach ($cid in $connAll.Keys) {
    $groups = $connAll[$cid].Groups
    $blockedIn = if ($groups.ContainsKey('Blocked')) { $groups['Blocked'].Count } else { 0 }
    $allowedIn = (@($groups.Keys | Where-Object { $_ -ne 'Blocked' } | ForEach-Object { $groups[$_].Count }) | Measure-Object -Sum).Sum
    if ($groups.Keys.Count -gt 1 -and $blockedIn -gt 0) {
        $detail = ($groups.Keys | ForEach-Object { "$($_): $($groups[$_] -join ', ')" }) -join " | "
        $inconsistency.Add([PSCustomObject]@{
            ConnectorId=$cid; ConnectorName=$connAll[$cid].Name
            DistinctDataGroups=($groups.Keys -join "; ")
            BlockedInPolicies=$blockedIn; AllowedInPolicies=$allowedIn
            Detail=$detail
            Note="Connector is Blocked in some policies but allowed in others - likely block gap"
            CollectedAt=$timestamp
        })
    }
}
$inconsistency | Sort-Object -Property AllowedInPolicies -Descending |
    Export-Csv "$OutputPath/DlpConnectorInconsistency.csv" -NoTypeInformation

# ---- 3e. Optional: baseline "should be blocked" gap report ----
$baselineGaps = [System.Collections.Generic.List[object]]::new()
if ($BlockedConnectorBaseline -and (Test-Path $BlockedConnectorBaseline)) {
    $baseList = @(Get-Content $BlockedConnectorBaseline |
        ForEach-Object { ($_ -split ',')[0].Trim() } |
        Where-Object { $_ -and $_ -notmatch '^#' })
    foreach ($polId in $policyIds) {
        $cm = $policyModel[$polId].Connectors
        foreach ($bc in $baseList) {
            $current = if ($cm.ContainsKey($bc)) { $cm[$bc] } else { "(not listed -> default: $($policyModel[$polId].DefaultGroup))" }
            if ($current -ne 'Blocked') {
                $baselineGaps.Add([PSCustomObject]@{
                    PolicyId=$polId; PolicyName=$policyModel[$polId].Name
                    ConnectorId=$bc; CurrentClassification=$current
                    Expected="Blocked"; CollectedAt=$timestamp
                })
            }
        }
    }
    $baselineGaps | Export-Csv "$OutputPath/DlpBlockGaps.csv" -NoTypeInformation
    Write-Host "  Baseline gap report: $($baselineGaps.Count) (policy x connector) gaps" -ForegroundColor Gray
}

# ============================================================================
# SUMMARY
# ============================================================================

$noCoverage = @($coverage | Where-Object { $_.CoverageStatus -like 'GAP*' })
$overlapEnv = @($coverage | Where-Object { $_.CoverageStatus -like 'OVERLAP*' })
$exactDupes = @($dupes | Where-Object { $_.IsExactDuplicate })
$disabled   = @($allDlpPolicies | Where-Object { -not $_.IsEnabled })
$totalElapsed = (Get-Date) - $startTime

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " DLP audit complete  ($("{0:hh\:mm\:ss}" -f $totalElapsed))" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Policies:                       $($allDlpPolicies.Count)  (disabled: $($disabled.Count))" -ForegroundColor Gray
Write-Host "  Environments:                   $($environments.Count)" -ForegroundColor Gray
Write-Host "  Environments with NO policy:    $($noCoverage.Count)   <- coverage gaps" -ForegroundColor $(if ($noCoverage.Count) {'Yellow'} else {'Gray'})
Write-Host "  Environments w/ multiple scoped:$($overlapEnv.Count)   <- overlaps" -ForegroundColor $(if ($overlapEnv.Count) {'Yellow'} else {'Gray'})
Write-Host "  Overlapping policy pairs:       $($overlap.Count)" -ForegroundColor Gray
Write-Host "  Exact-duplicate policy pairs:   $($exactDupes.Count)" -ForegroundColor $(if ($exactDupes.Count) {'Yellow'} else {'Gray'})
Write-Host "  Connector block-inconsistencies:$($inconsistency.Count)" -ForegroundColor $(if ($inconsistency.Count) {'Yellow'} else {'Gray'})
Write-Host ""
Write-Host "Output files in: $OutputPath" -ForegroundColor Cyan
Get-ChildItem "$OutputPath/*.csv" | ForEach-Object {
    Write-Host ("  {0,-34} {1,8:N1} KB" -f $_.Name, ($_.Length / 1KB)) -ForegroundColor Gray
}
Write-Host ""
Write-Host "Cleanup-job reading order:" -ForegroundColor Yellow
Write-Host "  1. EnvironmentDlpCoverage.csv   - which policy governs each env; GAP rows = no policy" -ForegroundColor Gray
Write-Host "  2. DlpPolicyOverlap.csv         - policies fighting over the same environments" -ForegroundColor Gray
Write-Host "  3. DlpDuplicatePolicies.csv     - merge/delete candidates (IsExactDuplicate first)" -ForegroundColor Gray
Write-Host "  4. DlpConnectorInconsistency.csv- connectors blocked in some policies but not others" -ForegroundColor Gray
Write-Host "  5. DlpPolicies / DlpConnectorRules / DlpPolicyEnvironments - supporting detail" -ForegroundColor Gray
Write-Host ""
