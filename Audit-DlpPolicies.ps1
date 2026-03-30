<#
.SYNOPSIS
    Audits DLP policies for duplicates, conflicts, and cleanup opportunities.
.DESCRIPTION
    Reads DlpPolicies.csv and DlpConnectorRules.csv (produced by powerplatform.ps1
    or Fix-DlpData.ps1) and generates an audit report. No API calls are made.

    Analyses performed:
    1. Policy Overview — counts, scopes, classification totals
    2. Duplicate Detection — exact and near-duplicate (>90% Jaccard) policies
    3. Conflict Detection — connectors classified differently across policies
    4. Connector Classification Matrix — cross-reference table
    5. Coverage Gaps — missing connectors, outlier policies
    6. Recommendations — prioritized action items

    Outputs:
    - Color-coded console summary
    - DlpAuditReport.html (self-contained, printable)
    - DlpConflicts.csv (filterable in Excel)
.PARAMETER InputPath
    Folder containing DlpPolicies.csv and DlpConnectorRules.csv.
.PARAMETER OutputPath
    Where to write report files. Defaults to InputPath.
.PARAMETER SkipHtml
    Console + CSV only, skip HTML report generation.
.EXAMPLE
    .\Audit-DlpPolicies.ps1 -InputPath .\PowerPlatformExport
    .\Audit-DlpPolicies.ps1 -InputPath .\Export -OutputPath .\Reports
    .\Audit-DlpPolicies.ps1 -InputPath .\Export -SkipHtml
#>

param(
    [Parameter(Mandatory)]
    [string]$InputPath,
    [string]$OutputPath,
    [switch]$SkipHtml
)

$ErrorActionPreference = "Stop"

if (-not $OutputPath) { $OutputPath = $InputPath }

# ============================================================================
# VALIDATE INPUTS
# ============================================================================

$policiesCsv = Join-Path $InputPath "DlpPolicies.csv"
$rulesCsv    = Join-Path $InputPath "DlpConnectorRules.csv"

if (-not (Test-Path $policiesCsv)) {
    Write-Host "ERROR: $policiesCsv not found." -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $rulesCsv)) {
    Write-Host "ERROR: $rulesCsv not found." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
}

# ============================================================================
# LOAD DATA
# ============================================================================

Write-Host "`n=== DLP Policy Audit ===" -ForegroundColor Cyan
Write-Host "Input:  $InputPath" -ForegroundColor Gray
Write-Host "Output: $OutputPath`n" -ForegroundColor Gray

$policies = Import-Csv $policiesCsv
$rules    = Import-Csv $rulesCsv

Write-Host "Loaded $($policies.Count) policies, $($rules.Count) connector rules.`n" -ForegroundColor Gray

if ($policies.Count -eq 0) {
    Write-Host "No policies found. Nothing to audit." -ForegroundColor Yellow
    exit 0
}

# Build lookup tables
$rulesByPolicy = @{}
foreach ($r in $rules) {
    if (-not $rulesByPolicy.ContainsKey($r.PolicyId)) {
        $rulesByPolicy[$r.PolicyId] = [System.Collections.Generic.List[object]]::new()
    }
    $rulesByPolicy[$r.PolicyId].Add($r)
}

# ============================================================================
# 1. POLICY OVERVIEW
# ============================================================================

Write-Host "[1/6] Policy Overview" -ForegroundColor Yellow

$enabledCount  = @($policies | Where-Object { $_.IsEnabled -eq "True" -or $_.IsEnabled -eq "TRUE" }).Count
$disabledCount = $policies.Count - $enabledCount

$scopeGroups = $policies | Group-Object EnvironmentScope
$classGroups = $rules | Group-Object Classification

$policyStats = foreach ($p in $policies) {
    $pRules = if ($rulesByPolicy.ContainsKey($p.PolicyId)) { $rulesByPolicy[$p.PolicyId] } else { @() }
    $classBreakdown = $pRules | Group-Object Classification
    [PSCustomObject]@{
        PolicyId       = $p.PolicyId
        DisplayName    = $p.DisplayName
        IsEnabled      = $p.IsEnabled
        Scope          = $p.EnvironmentScope
        TotalConnectors = $pRules.Count
        Business       = @($pRules | Where-Object { $_.Classification -eq "Business" }).Count
        NonBusiness    = @($pRules | Where-Object { $_.Classification -eq "NonBusiness" }).Count
        Blocked        = @($pRules | Where-Object { $_.Classification -eq "Blocked" }).Count
    }
}

Write-Host "  Total policies:    $($policies.Count)" -ForegroundColor Cyan
Write-Host "  Enabled:           $enabledCount" -ForegroundColor Green
Write-Host "  Disabled:          $disabledCount" -ForegroundColor $(if ($disabledCount -gt 0) { "DarkYellow" } else { "Green" })
Write-Host "  Scope breakdown:" -ForegroundColor Cyan
foreach ($sg in $scopeGroups) {
    Write-Host "    $($sg.Name): $($sg.Count)" -ForegroundColor Gray
}
Write-Host "  Classification totals:" -ForegroundColor Cyan
foreach ($cg in $classGroups) {
    Write-Host "    $($cg.Name): $($cg.Count)" -ForegroundColor Gray
}
Write-Host "  Per-policy connector counts:" -ForegroundColor Cyan
foreach ($ps in $policyStats) {
    $color = if ($ps.TotalConnectors -eq 0) { "Red" } else { "Gray" }
    Write-Host "    $($ps.DisplayName): $($ps.TotalConnectors) (B:$($ps.Business) NB:$($ps.NonBusiness) Blk:$($ps.Blocked))" -ForegroundColor $color
}

# ============================================================================
# 2. DUPLICATE DETECTION
# ============================================================================

Write-Host "`n[2/6] Duplicate Detection" -ForegroundColor Yellow

# Fingerprint each policy: sorted set of "ConnectorId:Classification"
$fingerprints = @{}
$fingerprintSets = @{}

foreach ($p in $policies) {
    $pRules = if ($rulesByPolicy.ContainsKey($p.PolicyId)) { $rulesByPolicy[$p.PolicyId] } else { @() }
    $pairs = @($pRules | ForEach-Object { "$($_.ConnectorId):$($_.Classification)" } | Sort-Object)
    $fp = ($pairs -join "|")
    $fingerprints[$p.PolicyId] = $fp
    $set = [System.Collections.Generic.HashSet[string]]::new([string[]]$pairs)
    $fingerprintSets[$p.PolicyId] = $set
}

# Find exact duplicates (same fingerprint)
$fpGroups = @{}
foreach ($kvp in $fingerprints.GetEnumerator()) {
    if ($kvp.Value -eq "") { continue }
    if (-not $fpGroups.ContainsKey($kvp.Value)) {
        $fpGroups[$kvp.Value] = [System.Collections.Generic.List[string]]::new()
    }
    $fpGroups[$kvp.Value].Add($kvp.Key)
}

$exactDuplicates = @($fpGroups.GetEnumerator() | Where-Object { $_.Value.Count -gt 1 })

if ($exactDuplicates.Count -gt 0) {
    Write-Host "  EXACT DUPLICATES FOUND: $($exactDuplicates.Count) group(s)" -ForegroundColor Red
    foreach ($group in $exactDuplicates) {
        $names = $group.Value | ForEach-Object {
            $pid = $_
            ($policies | Where-Object { $_.PolicyId -eq $pid }).DisplayName
        }
        Write-Host "    Group: $($names -join ', ')" -ForegroundColor Red
    }
}
else {
    Write-Host "  No exact duplicates found." -ForegroundColor Green
}

# Find near-duplicates (Jaccard > 0.9)
$nearDuplicates = [System.Collections.Generic.List[object]]::new()
$policyIds = @($policies.PolicyId)

for ($i = 0; $i -lt $policyIds.Count; $i++) {
    for ($j = $i + 1; $j -lt $policyIds.Count; $j++) {
        $idA = $policyIds[$i]
        $idB = $policyIds[$j]
        $setA = $fingerprintSets[$idA]
        $setB = $fingerprintSets[$idB]

        if ($setA.Count -eq 0 -and $setB.Count -eq 0) { continue }
        if ($setA.Count -eq 0 -or $setB.Count -eq 0) { continue }

        # Check if already an exact duplicate
        if ($fingerprints[$idA] -eq $fingerprints[$idB]) { continue }

        # Jaccard similarity
        $intersection = [System.Collections.Generic.HashSet[string]]::new($setA)
        $intersection.IntersectWith($setB)
        $union = [System.Collections.Generic.HashSet[string]]::new($setA)
        $union.UnionWith($setB)

        $jaccard = $intersection.Count / $union.Count

        if ($jaccard -gt 0.9) {
            $onlyA = [System.Collections.Generic.HashSet[string]]::new($setA)
            $onlyA.ExceptWith($setB)
            $onlyB = [System.Collections.Generic.HashSet[string]]::new($setB)
            $onlyB.ExceptWith($setA)

            $nearDuplicates.Add([PSCustomObject]@{
                PolicyIdA   = $idA
                PolicyNameA = ($policies | Where-Object { $_.PolicyId -eq $idA }).DisplayName
                PolicyIdB   = $idB
                PolicyNameB = ($policies | Where-Object { $_.PolicyId -eq $idB }).DisplayName
                Jaccard     = [math]::Round($jaccard, 4)
                OnlyInA     = ($onlyA -join "; ")
                OnlyInB     = ($onlyB -join "; ")
            })
        }
    }
}

if ($nearDuplicates.Count -gt 0) {
    Write-Host "  NEAR-DUPLICATES (>90% Jaccard): $($nearDuplicates.Count) pair(s)" -ForegroundColor Yellow
    foreach ($nd in $nearDuplicates) {
        Write-Host "    $($nd.PolicyNameA) <-> $($nd.PolicyNameB) (Jaccard: $($nd.Jaccard))" -ForegroundColor Yellow
        if ($nd.OnlyInA) { Write-Host "      Only in $($nd.PolicyNameA): $($nd.OnlyInA)" -ForegroundColor DarkYellow }
        if ($nd.OnlyInB) { Write-Host "      Only in $($nd.PolicyNameB): $($nd.OnlyInB)" -ForegroundColor DarkYellow }
    }
}
else {
    Write-Host "  No near-duplicates found." -ForegroundColor Green
}

# ============================================================================
# 3. CONFLICT DETECTION
# ============================================================================

Write-Host "`n[3/6] Conflict Detection" -ForegroundColor Yellow

$rulesByConnector = $rules | Group-Object ConnectorId
$conflicts = [System.Collections.Generic.List[object]]::new()

foreach ($cg in $rulesByConnector) {
    $classifications = @($cg.Group | Select-Object -ExpandProperty Classification -Unique)
    if ($classifications.Count -gt 1) {
        foreach ($r in $cg.Group) {
            $conflictingPolicies = @($cg.Group | Where-Object {
                $_.Classification -ne $r.Classification
            } | ForEach-Object { $_.PolicyId }) | Select-Object -Unique

            $conflicts.Add([PSCustomObject]@{
                ConnectorId     = $r.ConnectorId
                ConnectorName   = $r.ConnectorName
                Classification  = $r.Classification
                PolicyId        = $r.PolicyId
                PolicyName      = $r.PolicyName
                ConflictingWith = ($conflictingPolicies -join "; ")
            })
        }
    }
}

$conflictConnectors = @($conflicts | Select-Object -ExpandProperty ConnectorId -Unique)

if ($conflicts.Count -gt 0) {
    Write-Host "  CONFLICTS FOUND: $($conflictConnectors.Count) connector(s) classified differently" -ForegroundColor Red
    foreach ($connId in $conflictConnectors) {
        $connConflicts = @($conflicts | Where-Object { $_.ConnectorId -eq $connId })
        $connName = $connConflicts[0].ConnectorName
        Write-Host "    $connName ($connId):" -ForegroundColor Red
        $byPolicy = $connConflicts | Group-Object PolicyName
        foreach ($bp in $byPolicy) {
            $cls = ($bp.Group | Select-Object -First 1).Classification
            Write-Host "      $($bp.Name): $cls" -ForegroundColor DarkYellow
        }
    }
}
else {
    Write-Host "  No conflicts found." -ForegroundColor Green
}

# Export conflicts CSV
$conflictsCsvPath = Join-Path $OutputPath "DlpConflicts.csv"
if ($conflicts.Count -gt 0) {
    $conflicts | Export-Csv $conflictsCsvPath -NoTypeInformation
    Write-Host "  Conflicts exported to: $conflictsCsvPath" -ForegroundColor Gray
}
else {
    # Write empty CSV with headers
    [PSCustomObject]@{
        ConnectorId = ""; ConnectorName = ""; Classification = ""
        PolicyId = ""; PolicyName = ""; ConflictingWith = ""
    } | Export-Csv $conflictsCsvPath -NoTypeInformation
    # Remove the data row, keep header
    $header = (Get-Content $conflictsCsvPath)[0]
    $header | Set-Content $conflictsCsvPath
    Write-Host "  Empty conflicts CSV (header only) written to: $conflictsCsvPath" -ForegroundColor Gray
}

# ============================================================================
# 4. CONNECTOR CLASSIFICATION MATRIX
# ============================================================================

Write-Host "`n[4/6] Connector Classification Matrix" -ForegroundColor Yellow

$allConnectorIds = @($rules | Select-Object -ExpandProperty ConnectorId -Unique | Sort-Object)
$allPolicyIds    = @($policies | Select-Object -ExpandProperty PolicyId)

# Build matrix: connectorId -> policyId -> classification
$matrix = @{}
foreach ($r in $rules) {
    if (-not $matrix.ContainsKey($r.ConnectorId)) {
        $matrix[$r.ConnectorId] = @{}
    }
    $matrix[$r.ConnectorId][$r.PolicyId] = $r.Classification
}

# Build connector name lookup
$connectorNames = @{}
foreach ($r in $rules) {
    if (-not $connectorNames.ContainsKey($r.ConnectorId)) {
        $connectorNames[$r.ConnectorId] = $r.ConnectorName
    }
}

$matrixConnectorCount = $allConnectorIds.Count
$matrixPolicyCount    = $allPolicyIds.Count
Write-Host "  Matrix: $matrixConnectorCount connectors x $matrixPolicyCount policies" -ForegroundColor Cyan
$conflictingInMatrix = @($allConnectorIds | Where-Object { $conflictConnectors -contains $_ })
if ($conflictingInMatrix.Count -gt 0) {
    Write-Host "  Rows with conflicts: $($conflictingInMatrix.Count)" -ForegroundColor Red
}

# ============================================================================
# 5. COVERAGE GAPS
# ============================================================================

Write-Host "`n[5/6] Coverage Gaps" -ForegroundColor Yellow

# Connectors present in some policies but missing from others
$coverageGaps = [System.Collections.Generic.List[object]]::new()
foreach ($connId in $allConnectorIds) {
    $presentIn = @($allPolicyIds | Where-Object { $matrix[$connId].ContainsKey($_) })
    $missingFrom = @($allPolicyIds | Where-Object { -not $matrix[$connId].ContainsKey($_) })
    if ($missingFrom.Count -gt 0 -and $presentIn.Count -gt 0) {
        $coverageGaps.Add([PSCustomObject]@{
            ConnectorId   = $connId
            ConnectorName = $connectorNames[$connId]
            PresentIn     = $presentIn.Count
            MissingFrom   = $missingFrom.Count
            MissingPolicies = ($missingFrom | ForEach-Object {
                $pid = $_
                ($policies | Where-Object { $_.PolicyId -eq $pid }).DisplayName
            }) -join "; "
        })
    }
}

if ($coverageGaps.Count -gt 0) {
    Write-Host "  $($coverageGaps.Count) connector(s) have inconsistent coverage" -ForegroundColor Yellow
    $topGaps = $coverageGaps | Sort-Object MissingFrom -Descending | Select-Object -First 10
    foreach ($gap in $topGaps) {
        Write-Host "    $($gap.ConnectorName): present in $($gap.PresentIn), missing from $($gap.MissingFrom)" -ForegroundColor DarkYellow
    }
    if ($coverageGaps.Count -gt 10) {
        Write-Host "    ... and $($coverageGaps.Count - 10) more" -ForegroundColor DarkGray
    }
}
else {
    Write-Host "  All connectors have consistent coverage." -ForegroundColor Green
}

# Outlier policies (connector count >2 stddev from mean)
$connectorCounts = @($policyStats | ForEach-Object { $_.TotalConnectors })
$mean = ($connectorCounts | Measure-Object -Average).Average
$sumSqDiff = 0
foreach ($c in $connectorCounts) { $sumSqDiff += [math]::Pow($c - $mean, 2) }
$stddev = if ($connectorCounts.Count -gt 1) { [math]::Sqrt($sumSqDiff / ($connectorCounts.Count - 1)) } else { 0 }

$outlierPolicies = @()
if ($stddev -gt 0) {
    $lowerBound = $mean - (2 * $stddev)
    $upperBound = $mean + (2 * $stddev)
    $outlierPolicies = @($policyStats | Where-Object {
        $_.TotalConnectors -lt $lowerBound -or $_.TotalConnectors -gt $upperBound
    })
}

$emptyPolicies = @($policyStats | Where-Object { $_.TotalConnectors -eq 0 })

if ($outlierPolicies.Count -gt 0) {
    Write-Host "  Outlier policies (>2 stddev, mean=$([math]::Round($mean,1)), stddev=$([math]::Round($stddev,1))):" -ForegroundColor Yellow
    foreach ($op in $outlierPolicies) {
        Write-Host "    $($op.DisplayName): $($op.TotalConnectors) connectors" -ForegroundColor DarkYellow
    }
}
else {
    Write-Host "  No outlier policies detected (mean=$([math]::Round($mean,1)), stddev=$([math]::Round($stddev,1)))." -ForegroundColor Green
}

if ($emptyPolicies.Count -gt 0) {
    Write-Host "  Empty policies (0 connectors): $($emptyPolicies.Count)" -ForegroundColor Red
    foreach ($ep in $emptyPolicies) {
        Write-Host "    $($ep.DisplayName)" -ForegroundColor DarkYellow
    }
}

# ============================================================================
# 6. RECOMMENDATIONS
# ============================================================================

Write-Host "`n[6/6] Recommendations" -ForegroundColor Yellow

$recommendations = [System.Collections.Generic.List[object]]::new()

# Critical: Conflicts
if ($conflictConnectors.Count -gt 0) {
    $recommendations.Add([PSCustomObject]@{
        Priority    = "Critical"
        Category    = "Conflict"
        Description = "$($conflictConnectors.Count) connector(s) classified differently across policies. This causes unpredictable DLP enforcement."
        Action      = "Align connector classifications across all policies. Review DlpConflicts.csv for details."
    })
}

# High: Exact duplicates
foreach ($group in $exactDuplicates) {
    $names = $group.Value | ForEach-Object {
        $pid = $_
        ($policies | Where-Object { $_.PolicyId -eq $pid }).DisplayName
    }
    $recommendations.Add([PSCustomObject]@{
        Priority    = "High"
        Category    = "Duplicate"
        Description = "Exact duplicate policies: $($names -join ', ')"
        Action      = "Keep one policy and remove the others. Identical policies add management overhead with no benefit."
    })
}

# Medium: Near-duplicates
foreach ($nd in $nearDuplicates) {
    $recommendations.Add([PSCustomObject]@{
        Priority    = "Medium"
        Category    = "Near-Duplicate"
        Description = "$($nd.PolicyNameA) and $($nd.PolicyNameB) are $([math]::Round($nd.Jaccard * 100, 1))% similar."
        Action      = "Review differences and consider merging. Only in $($nd.PolicyNameA): $($nd.OnlyInA). Only in $($nd.PolicyNameB): $($nd.OnlyInB)."
    })
}

# Medium: Empty policies
foreach ($ep in $emptyPolicies) {
    $recommendations.Add([PSCustomObject]@{
        Priority    = "Medium"
        Category    = "Empty Policy"
        Description = "'$($ep.DisplayName)' has 0 connector rules."
        Action      = "Add connector rules or remove the policy. Empty policies provide no DLP protection."
    })
}

# Low: Disabled policies
$disabledPolicies = @($policies | Where-Object { $_.IsEnabled -eq "False" -or $_.IsEnabled -eq "FALSE" })
foreach ($dp in $disabledPolicies) {
    $recommendations.Add([PSCustomObject]@{
        Priority    = "Low"
        Category    = "Disabled"
        Description = "'$($dp.DisplayName)' is disabled."
        Action      = "Re-enable if needed or remove to reduce clutter."
    })
}

if ($recommendations.Count -gt 0) {
    $byPriority = $recommendations | Group-Object Priority
    foreach ($pg in @("Critical", "High", "Medium", "Low")) {
        $items = @($recommendations | Where-Object { $_.Priority -eq $pg })
        if ($items.Count -eq 0) { continue }
        $color = switch ($pg) {
            "Critical" { "Red" }
            "High"     { "DarkYellow" }
            "Medium"   { "Yellow" }
            "Low"      { "DarkGray" }
        }
        Write-Host "  [$pg] $($items.Count) item(s):" -ForegroundColor $color
        foreach ($item in $items) {
            Write-Host "    - [$($item.Category)] $($item.Description)" -ForegroundColor $color
        }
    }
}
else {
    Write-Host "  No issues found. Policies look clean!" -ForegroundColor Green
}

# ============================================================================
# HTML REPORT
# ============================================================================

if (-not $SkipHtml) {
    Write-Host "`nGenerating HTML report..." -ForegroundColor Cyan

    $sb = [System.Text.StringBuilder]::new(65536)

    function Add-Html {
        param([string]$Text)
        [void]$sb.AppendLine($Text)
    }

    function ConvertTo-HtmlSafe {
        param([string]$Text)
        if (-not $Text) { return "" }
        return $Text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace('"', "&quot;")
    }

    function Get-ClassificationBadge {
        param([string]$Classification)
        $colors = @{
            "Business"    = "background:#107c10;color:#fff"
            "NonBusiness" = "background:#ca5010;color:#fff"
            "Blocked"     = "background:#d13438;color:#fff"
        }
        $style = if ($colors.ContainsKey($Classification)) { $colors[$Classification] }
                 else { "background:#8a8886;color:#fff" }
        return "<span class='badge' style='$style'>$(ConvertTo-HtmlSafe $Classification)</span>"
    }

    function Get-PriorityBadge {
        param([string]$Priority)
        $colors = @{
            "Critical" = "background:#d13438;color:#fff"
            "High"     = "background:#ca5010;color:#fff"
            "Medium"   = "background:#ffb900;color:#323130"
            "Low"      = "background:#8a8886;color:#fff"
        }
        $style = if ($colors.ContainsKey($Priority)) { $colors[$Priority] }
                 else { "background:#8a8886;color:#fff" }
        return "<span class='badge' style='$style'>$(ConvertTo-HtmlSafe $Priority)</span>"
    }

    $reportDate = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

    Add-Html "<!DOCTYPE html>"
    Add-Html "<html lang='en'>"
    Add-Html "<head>"
    Add-Html "<meta charset='UTF-8'>"
    Add-Html "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
    Add-Html "<title>DLP Policy Audit Report</title>"
    Add-Html "<style>"
    Add-Html @"
:root {
    --primary: #0078d4;
    --primary-dark: #106ebe;
    --success: #107c10;
    --warning: #ffb900;
    --danger: #d13438;
    --orange: #ca5010;
    --neutral-10: #faf9f8;
    --neutral-20: #f3f2f1;
    --neutral-30: #edebe9;
    --neutral-60: #8a8886;
    --neutral-90: #323130;
    --neutral-100: #201f1e;
    --font: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: var(--font); background: var(--neutral-10); color: var(--neutral-90); line-height: 1.5; }
.container { max-width: 1400px; margin: 0 auto; padding: 24px; }
h1 { font-size: 28px; font-weight: 600; color: var(--neutral-100); margin-bottom: 4px; }
.subtitle { color: var(--neutral-60); font-size: 14px; margin-bottom: 32px; }
h2 { font-size: 20px; font-weight: 600; color: var(--neutral-100); margin: 32px 0 16px; padding-bottom: 8px; border-bottom: 2px solid var(--primary); }
h3 { font-size: 16px; font-weight: 600; color: var(--neutral-90); margin: 16px 0 8px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.kpi-card { background: #fff; border: 1px solid var(--neutral-30); border-radius: 8px; padding: 20px; text-align: center; }
.kpi-card .value { font-size: 36px; font-weight: 700; color: var(--primary); }
.kpi-card .label { font-size: 13px; color: var(--neutral-60); margin-top: 4px; }
.kpi-card.danger .value { color: var(--danger); }
.kpi-card.warning .value { color: var(--orange); }
.kpi-card.success .value { color: var(--success); }
table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid var(--neutral-30); border-radius: 8px; overflow: hidden; margin-bottom: 16px; }
th { background: var(--neutral-20); font-weight: 600; font-size: 13px; color: var(--neutral-90); text-align: left; padding: 10px 12px; white-space: nowrap; }
td { padding: 8px 12px; font-size: 13px; border-top: 1px solid var(--neutral-30); }
tr:hover { background: var(--neutral-10); }
tr.conflict-row { background: #fde7e9; }
tr.conflict-row:hover { background: #fad2d6; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; white-space: nowrap; }
.matrix-wrap { overflow-x: auto; margin-bottom: 16px; }
.matrix-wrap table { min-width: max-content; }
.matrix-wrap th:first-child, .matrix-wrap td:first-child { position: sticky; left: 0; background: #fff; z-index: 1; min-width: 250px; }
.matrix-wrap th:first-child { background: var(--neutral-20); }
.matrix-wrap tr.conflict-row td:first-child { background: #fde7e9; }
.cell-missing { color: var(--neutral-60); font-style: italic; }
.section-clean { color: var(--success); font-weight: 600; margin: 8px 0; }
.recommendation { background: #fff; border: 1px solid var(--neutral-30); border-radius: 8px; padding: 16px; margin-bottom: 12px; }
.recommendation .rec-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.recommendation .rec-category { font-weight: 600; color: var(--neutral-90); }
.recommendation .rec-desc { font-size: 14px; margin-bottom: 6px; }
.recommendation .rec-action { font-size: 13px; color: var(--neutral-60); }
.no-issues { text-align: center; padding: 40px; color: var(--success); font-size: 18px; font-weight: 600; }
@media print {
    body { background: #fff; }
    .container { max-width: 100%; padding: 0; }
    h2 { page-break-after: avoid; }
    table, .recommendation, .kpi-grid { page-break-inside: avoid; }
    .matrix-wrap { overflow: visible; }
    .matrix-wrap th:first-child, .matrix-wrap td:first-child { position: static; }
}
"@
    Add-Html "</style>"
    Add-Html "</head>"
    Add-Html "<body>"
    Add-Html "<div class='container'>"

    # Title
    Add-Html "<h1>DLP Policy Audit Report</h1>"
    Add-Html "<div class='subtitle'>Generated: $reportDate | Source: $(ConvertTo-HtmlSafe $InputPath)</div>"

    # --- Section 1: Overview KPI Cards ---
    Add-Html "<h2>1. Policy Overview</h2>"
    Add-Html "<div class='kpi-grid'>"
    Add-Html "<div class='kpi-card'><div class='value'>$($policies.Count)</div><div class='label'>Total Policies</div></div>"
    Add-Html "<div class='kpi-card success'><div class='value'>$enabledCount</div><div class='label'>Enabled</div></div>"
    $disabledClass = if ($disabledCount -gt 0) { "warning" } else { "success" }
    Add-Html "<div class='kpi-card $disabledClass'><div class='value'>$disabledCount</div><div class='label'>Disabled</div></div>"
    $conflictClass = if ($conflictConnectors.Count -gt 0) { "danger" } else { "success" }
    Add-Html "<div class='kpi-card $conflictClass'><div class='value'>$($conflictConnectors.Count)</div><div class='label'>Connector Conflicts</div></div>"
    $dupeClass = if ($exactDuplicates.Count -gt 0) { "danger" } else { "success" }
    Add-Html "<div class='kpi-card $dupeClass'><div class='value'>$($exactDuplicates.Count)</div><div class='label'>Duplicate Groups</div></div>"
    Add-Html "<div class='kpi-card'><div class='value'>$($allConnectorIds.Count)</div><div class='label'>Unique Connectors</div></div>"
    Add-Html "</div>"

    # Policy table
    Add-Html "<h3>Policy Details</h3>"
    Add-Html "<table>"
    Add-Html "<tr><th>Policy Name</th><th>Status</th><th>Scope</th><th>Total</th><th>Business</th><th>NonBusiness</th><th>Blocked</th></tr>"
    foreach ($ps in $policyStats) {
        $statusBadge = if ($ps.IsEnabled -eq "True" -or $ps.IsEnabled -eq "TRUE") {
            "<span class='badge' style='background:#107c10;color:#fff'>Enabled</span>"
        } else {
            "<span class='badge' style='background:#8a8886;color:#fff'>Disabled</span>"
        }
        Add-Html "<tr><td>$(ConvertTo-HtmlSafe $ps.DisplayName)</td><td>$statusBadge</td><td>$(ConvertTo-HtmlSafe $ps.Scope)</td><td>$($ps.TotalConnectors)</td><td>$($ps.Business)</td><td>$($ps.NonBusiness)</td><td>$($ps.Blocked)</td></tr>"
    }
    Add-Html "</table>"

    # --- Section 2: Duplicates ---
    Add-Html "<h2>2. Duplicate Detection</h2>"
    if ($exactDuplicates.Count -gt 0) {
        Add-Html "<h3>Exact Duplicates</h3>"
        Add-Html "<table>"
        Add-Html "<tr><th>Group</th><th>Policies</th><th>Connectors</th></tr>"
        $groupNum = 1
        foreach ($group in $exactDuplicates) {
            $names = $group.Value | ForEach-Object {
                $pid = $_
                ConvertTo-HtmlSafe ($policies | Where-Object { $_.PolicyId -eq $pid }).DisplayName
            }
            $connCount = $fingerprintSets[$group.Value[0]].Count
            Add-Html "<tr><td>$groupNum</td><td>$($names -join '<br>')</td><td>$connCount</td></tr>"
            $groupNum++
        }
        Add-Html "</table>"
    }
    else {
        Add-Html "<p class='section-clean'>No exact duplicates found.</p>"
    }

    if ($nearDuplicates.Count -gt 0) {
        Add-Html "<h3>Near-Duplicates (&gt;90% Jaccard Similarity)</h3>"
        Add-Html "<table>"
        Add-Html "<tr><th>Policy A</th><th>Policy B</th><th>Similarity</th><th>Only in A</th><th>Only in B</th></tr>"
        foreach ($nd in $nearDuplicates) {
            $pctStr = "$([math]::Round($nd.Jaccard * 100, 1))%"
            Add-Html "<tr><td>$(ConvertTo-HtmlSafe $nd.PolicyNameA)</td><td>$(ConvertTo-HtmlSafe $nd.PolicyNameB)</td><td>$pctStr</td><td>$(ConvertTo-HtmlSafe $nd.OnlyInA)</td><td>$(ConvertTo-HtmlSafe $nd.OnlyInB)</td></tr>"
        }
        Add-Html "</table>"
    }
    else {
        Add-Html "<p class='section-clean'>No near-duplicates found.</p>"
    }

    # --- Section 3: Conflicts ---
    Add-Html "<h2>3. Connector Conflicts</h2>"
    if ($conflictConnectors.Count -gt 0) {
        Add-Html "<table>"
        Add-Html "<tr><th>Connector</th><th>Connector ID</th><th>Classification</th><th>Policy</th><th>Conflicting With</th></tr>"
        foreach ($c in $conflicts) {
            Add-Html "<tr class='conflict-row'><td>$(ConvertTo-HtmlSafe $c.ConnectorName)</td><td>$(ConvertTo-HtmlSafe $c.ConnectorId)</td><td>$(Get-ClassificationBadge $c.Classification)</td><td>$(ConvertTo-HtmlSafe $c.PolicyName)</td><td>$(ConvertTo-HtmlSafe $c.ConflictingWith)</td></tr>"
        }
        Add-Html "</table>"
    }
    else {
        Add-Html "<p class='section-clean'>No conflicts found. All connectors are consistently classified.</p>"
    }

    # --- Section 4: Classification Matrix ---
    Add-Html "<h2>4. Connector Classification Matrix</h2>"
    Add-Html "<div class='matrix-wrap'>"
    Add-Html "<table>"

    # Header row: connector name | policy names...
    $headerHtml = "<tr><th>Connector</th>"
    foreach ($pid in $allPolicyIds) {
        $pName = ($policies | Where-Object { $_.PolicyId -eq $pid }).DisplayName
        $headerHtml += "<th>$(ConvertTo-HtmlSafe $pName)</th>"
    }
    $headerHtml += "</tr>"
    Add-Html $headerHtml

    # Data rows
    foreach ($connId in $allConnectorIds) {
        $connName = $connectorNames[$connId]
        $isConflict = $conflictConnectors -contains $connId
        $rowClass = if ($isConflict) { " class='conflict-row'" } else { "" }
        $rowHtml = "<tr$rowClass><td><strong>$(ConvertTo-HtmlSafe $connName)</strong><br><small style='color:var(--neutral-60)'>$(ConvertTo-HtmlSafe $connId)</small></td>"
        foreach ($pid in $allPolicyIds) {
            if ($matrix[$connId].ContainsKey($pid)) {
                $cls = $matrix[$connId][$pid]
                $rowHtml += "<td>$(Get-ClassificationBadge $cls)</td>"
            }
            else {
                $rowHtml += "<td><span class='cell-missing'>--</span></td>"
            }
        }
        $rowHtml += "</tr>"
        Add-Html $rowHtml
    }

    Add-Html "</table>"
    Add-Html "</div>"

    # --- Section 5: Coverage Gaps ---
    Add-Html "<h2>5. Coverage Gaps</h2>"
    if ($coverageGaps.Count -gt 0) {
        Add-Html "<h3>Connectors with Inconsistent Coverage</h3>"
        Add-Html "<table>"
        Add-Html "<tr><th>Connector</th><th>Connector ID</th><th>Present In</th><th>Missing From</th><th>Missing Policies</th></tr>"
        $sortedGaps = $coverageGaps | Sort-Object MissingFrom -Descending
        foreach ($gap in $sortedGaps) {
            Add-Html "<tr><td>$(ConvertTo-HtmlSafe $gap.ConnectorName)</td><td>$(ConvertTo-HtmlSafe $gap.ConnectorId)</td><td>$($gap.PresentIn)</td><td>$($gap.MissingFrom)</td><td>$(ConvertTo-HtmlSafe $gap.MissingPolicies)</td></tr>"
        }
        Add-Html "</table>"
    }
    else {
        Add-Html "<p class='section-clean'>All connectors have consistent coverage across policies.</p>"
    }

    if ($outlierPolicies.Count -gt 0 -or $emptyPolicies.Count -gt 0) {
        Add-Html "<h3>Outlier Policies</h3>"
        Add-Html "<table>"
        Add-Html "<tr><th>Policy</th><th>Connectors</th><th>Issue</th></tr>"
        foreach ($ep in $emptyPolicies) {
            Add-Html "<tr class='conflict-row'><td>$(ConvertTo-HtmlSafe $ep.DisplayName)</td><td>0</td><td>Empty policy (no connector rules)</td></tr>"
        }
        foreach ($op in $outlierPolicies) {
            if ($op.TotalConnectors -eq 0) { continue } # already shown as empty
            $direction = if ($op.TotalConnectors -lt $mean) { "unusually few" } else { "unusually many" }
            Add-Html "<tr><td>$(ConvertTo-HtmlSafe $op.DisplayName)</td><td>$($op.TotalConnectors)</td><td>$direction connectors (mean: $([math]::Round($mean,1)))</td></tr>"
        }
        Add-Html "</table>"
    }

    # --- Section 6: Recommendations ---
    Add-Html "<h2>6. Recommendations</h2>"
    if ($recommendations.Count -gt 0) {
        foreach ($pg in @("Critical", "High", "Medium", "Low")) {
            $items = @($recommendations | Where-Object { $_.Priority -eq $pg })
            if ($items.Count -eq 0) { continue }
            foreach ($item in $items) {
                Add-Html "<div class='recommendation'>"
                Add-Html "  <div class='rec-header'>$(Get-PriorityBadge $item.Priority) <span class='rec-category'>$(ConvertTo-HtmlSafe $item.Category)</span></div>"
                Add-Html "  <div class='rec-desc'>$(ConvertTo-HtmlSafe $item.Description)</div>"
                Add-Html "  <div class='rec-action'>$(ConvertTo-HtmlSafe $item.Action)</div>"
                Add-Html "</div>"
            }
        }
    }
    else {
        Add-Html "<div class='no-issues'>No issues found. Your DLP policies look clean!</div>"
    }

    Add-Html "</div>"
    Add-Html "</body>"
    Add-Html "</html>"

    # Write HTML file with UTF-8 no BOM
    $htmlPath = Join-Path $OutputPath "DlpAuditReport.html"
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($htmlPath, $sb.ToString(), $utf8NoBom)
    Write-Host "  HTML report written to: $htmlPath" -ForegroundColor Green
}

# ============================================================================
# SUMMARY
# ============================================================================

Write-Host "`n=== Audit Complete ===" -ForegroundColor Cyan
Write-Host "  Conflicts CSV: $conflictsCsvPath" -ForegroundColor Gray
if (-not $SkipHtml) {
    Write-Host "  HTML Report:   $(Join-Path $OutputPath 'DlpAuditReport.html')" -ForegroundColor Gray
}

$totalIssues = $recommendations.Count
if ($totalIssues -gt 0) {
    $critCount = @($recommendations | Where-Object { $_.Priority -eq "Critical" }).Count
    $highCount = @($recommendations | Where-Object { $_.Priority -eq "High" }).Count
    Write-Host "  Total issues:  $totalIssues ($critCount critical, $highCount high)" -ForegroundColor $(if ($critCount -gt 0) { "Red" } elseif ($highCount -gt 0) { "DarkYellow" } else { "Yellow" })
}
else {
    Write-Host "  No issues found." -ForegroundColor Green
}

Write-Host "`nDone.`n" -ForegroundColor Cyan
