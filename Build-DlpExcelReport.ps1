<#
.SYNOPSIS
    Builds a DLP cleanup triage workbook (DlpAudit.xlsx) from the CSVs
    produced by powerplatform-dlp.ps1.
.DESCRIPTION
    Requires the ImportExcel module (already present: v7.x).
    Produces one workbook:
      - Triage      ranked findings with blank Decision/Owner/Notes columns
      - Coverage    EnvironmentDlpCoverage (GAP/OVERLAP/Covered color-coded)
      - Overlap     DlpPolicyOverlap
      - Duplicates  DlpDuplicatePolicies
      - Inconsist.  DlpConnectorInconsistency
      - Policies    DlpPolicies
      - PolicyEnvs  DlpPolicyEnvironments
      - Connectors  DlpConnectorRules
      - Environments
    Actionable sheets get appended Decision / Owner / Notes columns so the
    cleanup can be worked and tracked in the file itself.
.PARAMETER OutputPath
    Folder containing the audit CSVs. Default C:\users\jcollins\dlpoutput
.PARAMETER ReportPath
    Output .xlsx path. Default <OutputPath>\DlpAudit.xlsx
.EXAMPLE
    .\Build-DlpExcelReport.ps1
    .\Build-DlpExcelReport.ps1 -OutputPath C:\users\jcollins\dlpoutput
#>

param(
    [string]$OutputPath = "C:\users\jcollins\dlpoutput",
    [string]$ReportPath = ""
)

$ErrorActionPreference = "Stop"
if (-not $ReportPath) { $ReportPath = Join-Path $OutputPath "DlpAudit.xlsx" }

if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    throw "ImportExcel module not found. Install with: Install-Module ImportExcel -Scope CurrentUser"
}
Import-Module ImportExcel

function Import-CsvSafe {
    param([string]$Name)
    $p = Join-Path $OutputPath $Name
    if (-not (Test-Path $p) -or (Get-Item $p).Length -eq 0) { return @() }
    @(Import-Csv $p)
}

Write-Host "Reading CSVs from $OutputPath ..." -ForegroundColor Cyan
$cov   = Import-CsvSafe "EnvironmentDlpCoverage.csv"
$ovl   = Import-CsvSafe "DlpPolicyOverlap.csv"
$dup   = Import-CsvSafe "DlpDuplicatePolicies.csv"
$inc   = Import-CsvSafe "DlpConnectorInconsistency.csv"
$pol   = Import-CsvSafe "DlpPolicies.csv"
$polEnv= Import-CsvSafe "DlpPolicyEnvironments.csv"
$rules = Import-CsvSafe "DlpConnectorRules.csv"
$envs  = Import-CsvSafe "Environments.csv"

if ($pol.Count -eq 0) { throw "DlpPolicies.csv is empty - run powerplatform-dlp.ps1 first." }

# ---- compute triage findings ----
$gapEnvs   = @($cov | Where-Object { $_.CoverageStatus -like 'GAP*' })
$ovlEnvs   = @($cov | Where-Object { $_.CoverageStatus -like 'OVERLAP*' })
$exactDup  = @($dup | Where-Object { "$($_.IsExactDuplicate)" -eq 'True' })
$nearDup   = @($dup | Where-Object { "$($_.IsExactDuplicate)" -ne 'True' })
$disabled  = @($pol | Where-Object { "$($_.IsEnabled)" -eq 'False' })
$blockNone = @($pol | Where-Object { [int]("0"+$_.BlockedConnectors) -eq 0 })
$dupDefault= @($pol | Where-Object { "$($_.IsTenantWideDefault)" -eq 'True' })

$timestamp = if ($pol[0].CollectedAt) { $pol[0].CollectedAt } else { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }

$triage = @(
    [PSCustomObject]@{ Priority=1; Severity="HIGH";   Finding="Environments with NO DLP policy";                 Count=$gapEnvs.Count;   WhereToLook="Coverage (CoverageStatus = GAP)";           Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=2; Severity="HIGH";   Finding="Environments under multiple scoped/default policies"; Count=$ovlEnvs.Count; WhereToLook="Coverage (OVERLAP) + Overlap sheet";       Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=3; Severity="HIGH";   Finding="Tenant-wide (All/Except) policies (only one should exist)"; Count=$dupDefault.Count; WhereToLook="Policies (IsTenantWideDefault=True)"; Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=4; Severity="MEDIUM"; Finding="Exact-duplicate policy pairs (safe merge/delete)";  Count=$exactDup.Count;  WhereToLook="Duplicates (IsExactDuplicate=True)";        Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=5; Severity="MEDIUM"; Finding="Policies that block NOTHING (permissive)";          Count=$blockNone.Count; WhereToLook="Policies (BlockedConnectors=0)";              Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=6; Severity="MEDIUM"; Finding="Connectors blocked in some policies, allowed in others"; Count=$inc.Count; WhereToLook="Inconsistency sheet";                       Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=7; Severity="LOW";    Finding="Overlapping policy pairs (any shared environment)"; Count=$ovl.Count;      WhereToLook="Overlap sheet";                             Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=8; Severity="LOW";    Finding="Near-duplicate policy pairs (review for merge)";    Count=$nearDup.Count;   WhereToLook="Duplicates (IsExactDuplicate=False)";       Decision=""; Owner=""; Notes="" }
    [PSCustomObject]@{ Priority=9; Severity="INFO";   Finding="Disabled policies (consider deleting)";             Count=$disabled.Count;  WhereToLook="Policies (IsEnabled=False)";                Decision=""; Owner=""; Notes="" }
)

$legend = @(
    [PSCustomObject]@{ Item="Generated";          Value=$timestamp }
    [PSCustomObject]@{ Item="Source CSVs";        Value=$OutputPath }
    [PSCustomObject]@{ Item="Total policies";     Value=$pol.Count }
    [PSCustomObject]@{ Item="Total environments"; Value=$cov.Count }
    [PSCustomObject]@{ Item="Classification map"; Value="hbi/Confidential = Business | lbi/General = Non-Business | blocked = Blocked" }
    [PSCustomObject]@{ Item="How to use";         Value="Work top-down by Priority. Record outcome in Decision/Owner/Notes on each sheet." }
)

# ---- append working columns to actionable sheets ----
function Add-WorkCols {
    param([array]$Rows)
    $Rows | ForEach-Object {
        $o = $_ | Select-Object *
        $o | Add-Member -NotePropertyName Decision -NotePropertyValue "" -Force
        $o | Add-Member -NotePropertyName Owner    -NotePropertyValue "" -Force
        $o | Add-Member -NotePropertyName Notes    -NotePropertyValue "" -Force
        $o
    }
}
$covW = Add-WorkCols $cov
$ovlW = Add-WorkCols $ovl
$dupW = Add-WorkCols $dup
$incW = Add-WorkCols $inc

if (Test-Path $ReportPath) { Remove-Item $ReportPath -Force }
$xl = @{ Path = $ReportPath; AutoSize = $true; FreezeTopRow = $true; BoldTopRow = $true; AutoFilter = $true }

Write-Host "Writing $ReportPath ..." -ForegroundColor Cyan

# Triage sheet with severity color coding
$ctTriage = @(
    New-ConditionalText -Text "HIGH"   -BackgroundColor '#F8CBAD' -ConditionalTextColor '#843C0C'
    New-ConditionalText -Text "MEDIUM" -BackgroundColor '#FFE699' -ConditionalTextColor '#7F6000'
    New-ConditionalText -Text "LOW"    -BackgroundColor '#D9E1F2' -ConditionalTextColor '#1F4E78'
    New-ConditionalText -Text "INFO"   -BackgroundColor '#E2EFDA' -ConditionalTextColor '#375623'
)
$triage | Export-Excel @xl -WorksheetName "Triage" -TableName "Triage" -TableStyle Medium2 -ConditionalText $ctTriage -Title "DLP Cleanup - Triage" -TitleBold -TitleSize 14
$legend | Export-Excel -Path $ReportPath -WorksheetName "Triage" -StartRow ($triage.Count + 4) -AutoSize -BoldTopRow

# Coverage with GAP/OVERLAP color coding
$ctCov = @(
    New-ConditionalText -Text "GAP"     -BackgroundColor '#FFC7CE' -ConditionalTextColor '#9C0006'
    New-ConditionalText -Text "OVERLAP" -BackgroundColor '#FFEB9C' -ConditionalTextColor '#9C6500'
    New-ConditionalText -Text "Covered" -BackgroundColor '#C6EFCE' -ConditionalTextColor '#006100'
)
$covW | Export-Excel @xl -WorksheetName "Coverage" -TableName "Coverage" -TableStyle Medium2 -ConditionalText $ctCov

function Export-Sheet {
    param([array]$Data, [string]$Sheet, [string]$Table, [array]$Ct)
    if ($Data.Count -eq 0) {
        [PSCustomObject]@{ Note = "No rows - nothing flagged for this category." } |
            Export-Excel -Path $ReportPath -WorksheetName $Sheet -AutoSize -BoldTopRow
        return
    }
    if ($Ct) { $Data | Export-Excel @xl -WorksheetName $Sheet -TableName $Table -TableStyle Medium2 -ConditionalText $Ct }
    else     { $Data | Export-Excel @xl -WorksheetName $Sheet -TableName $Table -TableStyle Medium2 }
}

Export-Sheet -Data $ovlW   -Sheet "Overlap"       -Table "Overlap"
Export-Sheet -Data $dupW   -Sheet "Duplicates"    -Table "Duplicates"
Export-Sheet -Data $incW   -Sheet "Inconsistency" -Table "Inconsistency"
Export-Sheet -Data $pol    -Sheet "Policies"      -Table "Policies"
Export-Sheet -Data $polEnv -Sheet "PolicyEnvs"    -Table "PolicyEnvs"
Export-Sheet -Data $rules  -Sheet "Connectors"    -Table "ConnectorRules"
Export-Sheet -Data $envs   -Sheet "Environments"  -Table "Environments"

Write-Host ""
Write-Host "Done -> $ReportPath" -ForegroundColor Green
Write-Host "Open the Triage sheet first; work findings top-down by Priority." -ForegroundColor Gray
