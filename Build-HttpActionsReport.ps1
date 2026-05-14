[CmdletBinding()]
param(
    [string] $ExportDir = (Split-Path -Parent $MyInvocation.MyCommand.Path),
    [string] $OutCsv    = $null,
    [string] $OutXlsx   = $null
)

if (-not $OutCsv)  { $OutCsv  = Join-Path $ExportDir 'HttpActionsReport.csv' }
if (-not $OutXlsx) { $OutXlsx = Join-Path $ExportDir 'HttpActionsReport.xlsx' }

$flowsCsv   = Join-Path $ExportDir 'Flows.csv'
$actionsCsv = Join-Path $ExportDir 'FlowActions.csv'

foreach ($p in @($flowsCsv, $actionsCsv)) {
    if (-not (Test-Path $p)) { throw "Missing input file: $p" }
}

Write-Host "Loading Flows.csv ..." -ForegroundColor Cyan
$flowsById = @{}
Import-Csv $flowsCsv | ForEach-Object { $flowsById[$_.FlowId] = $_ }

Write-Host "Loading FlowActions.csv ..." -ForegroundColor Cyan
$httpActions = Import-Csv $actionsCsv | Where-Object { $_.ActionType -eq 'Http' }

Write-Host ("Found {0} Http actions across {1} flows" -f `
    $httpActions.Count, ($httpActions | Select-Object -ExpandProperty FlowId -Unique).Count) -ForegroundColor Green

function Get-BaseUrl {
    param([string] $Url)
    if ([string]::IsNullOrWhiteSpace($Url)) { return '' }
    try {
        $u = [Uri] $Url
        if ($u.IsAbsoluteUri) {
            return ('{0}://{1}' -f $u.Scheme, $u.Authority)
        }
    } catch { }
    # Fallback: regex pull of scheme://host[:port]
    if ($Url -match '^(?<base>[a-zA-Z][a-zA-Z0-9+\-.]*://[^/\s?#]+)') {
        return $Matches['base']
    }
    return ''
}

$report = foreach ($a in $httpActions) {
    $flow = $flowsById[$a.FlowId]
    [pscustomobject]@{
        FlowName            = if ($flow) { $flow.DisplayName }       else { '' }
        FlowState           = if ($flow) { $flow.State }             else { '' }
        EnvironmentName     = if ($flow) { $flow.EnvironmentName }   else { '' }
        CreatorObjectId     = if ($flow) { $flow.CreatorObjectId }   else { '' }
        CreatorDisplayName  = if ($flow) { $flow.CreatorDisplayName } else { '' }
        CreatedTime         = if ($flow) { $flow.CreatedTime }       else { '' }
        LastModifiedTime    = if ($flow) { $flow.LastModifiedTime }  else { '' }
        ActionName          = $a.Name
        ActionPosition      = $a.Position
        ActionType          = $a.ActionType
        BaseUrl             = Get-BaseUrl $a.EndpointUrl
        EndpointUrl         = $a.EndpointUrl
        FlowId              = $a.FlowId
        EnvironmentId       = $a.EnvironmentId
    }
}

$report = $report |
    Sort-Object EnvironmentName, FlowName, { [int]$_.ActionPosition }

Write-Host "Writing CSV: $OutCsv" -ForegroundColor Cyan
$report | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8

# Summary stats
$flowCount   = ($report | Select-Object -ExpandProperty FlowId -Unique).Count
$baseUrls    = $report | Group-Object BaseUrl | Sort-Object Count -Descending
$creatorCnt  = ($report | Select-Object -ExpandProperty CreatorObjectId -Unique | Where-Object { $_ }).Count

Write-Host ""
Write-Host "=== Summary ===" -ForegroundColor Yellow
Write-Host ("Http actions:      {0}" -f $report.Count)
Write-Host ("Distinct flows:    {0}" -f $flowCount)
Write-Host ("Distinct creators: {0}" -f $creatorCnt)
Write-Host ""
Write-Host "Top base URLs:" -ForegroundColor Yellow
$baseUrls | Select-Object -First 15 | Format-Table Count, Name -AutoSize

# XLSX via ImportExcel
$haveImportExcel = Get-Module -ListAvailable -Name ImportExcel
if (-not $haveImportExcel) {
    Write-Host "Installing ImportExcel module for current user ..." -ForegroundColor Cyan
    try {
        Install-Module ImportExcel -Scope CurrentUser -Force -AllowClobber -ErrorAction Stop
        $haveImportExcel = $true
    } catch {
        Write-Warning "Could not install ImportExcel: $($_.Exception.Message). Skipping XLSX."
    }
}

if ($haveImportExcel) {
    Import-Module ImportExcel -ErrorAction Stop
    if (Test-Path $OutXlsx) { Remove-Item $OutXlsx -Force }

    Write-Host "Writing Excel: $OutXlsx" -ForegroundColor Cyan
    $report | Export-Excel -Path $OutXlsx `
        -WorksheetName 'HTTP Actions' `
        -AutoSize -AutoFilter -FreezeTopRow -BoldTopRow -TableStyle Medium2

    $baseUrls |
        Select-Object @{n='BaseUrl';e={$_.Name}}, Count |
        Export-Excel -Path $OutXlsx -WorksheetName 'BaseUrl Summary' `
            -AutoSize -AutoFilter -FreezeTopRow -BoldTopRow -TableStyle Medium4
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
