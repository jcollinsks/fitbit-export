<#
.SYNOPSIS
    Generates an Enterprise Governance Power BI Dashboard (PBIP) from Power Platform CSV exports.
.DESCRIPTION
    Creates a 16-page executive-grade governance PBIP project with:
    - ~100+ DAX measures (governance score, shadow IT rate, cross-table composites)
    - 14 tables including Connections (new), cross-table intelligence measures
    - Visual types: card, donut, bar, lineChart, treemap, gauge, slicer, columnChart, matrix, table
    - Narrative flow: Command Center → Domain Deep-Dives → Risk Intelligence → Detail/Drill-Down
    - Consistent 20px grid system, hero KPIs, insight charts, drill-down tables

    Tier 1 — Command Center: Executive Command Center
    Tier 2 — Domain Governance: Environments, Apps, Flows, Agents, Connectors/DLP, Endpoints
    Tier 3 — Risk & Intelligence: Risk/Shadow IT, Maker Activity, Connection Intelligence
    Tier 4 — Detail/Drill-Down: Environment, App, Flow, Agent, DLP Policy, Connector

    Open the generated .pbip file in Power BI Desktop (Developer Mode enabled).
.PARAMETER CsvPath
    Path to the folder containing CSV files from Collect-PowerPlatformData.ps1.
.PARAMETER OutputPath
    Where to create the PBIP project folder. Defaults to ./PowerPlatformReport2.
.EXAMPLE
    .\pbix2.ps1 -CsvPath C:\exports\PowerPlatformExport
    .\pbix2.ps1 -CsvPath .\PowerPlatformExport -OutputPath .\MyReport2
#>

param(
    [Parameter(Mandatory)]
    [string]$CsvPath,
    [string]$OutputPath = "./PowerPlatformReport2"
)

$ErrorActionPreference = "Stop"

# Resolve to absolute path with trailing backslash
$CsvPath = (Resolve-Path $CsvPath).Path.TrimEnd('\') + '\'

# Validate CSV files exist
$requiredCsvs = @("Environments.csv", "Apps.csv", "Flows.csv", "Connectors.csv", "Connections.csv")
$missingCsvs = $requiredCsvs | Where-Object { -not (Test-Path (Join-Path $CsvPath $_)) }
if ($missingCsvs.Count -gt 0) {
    Write-Host "ERROR: Missing required CSV files in $CsvPath" -ForegroundColor Red
    $missingCsvs | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host ""
    Write-Host "Run powerplatform.ps1 first to collect data, then point -CsvPath to the output folder." -ForegroundColor Yellow
    exit 1
}
Write-Host "CSV source: $CsvPath" -ForegroundColor Cyan

# ============================================================================
# HELPERS (copied from pbix.ps1)
# ============================================================================

function New-Guid { [guid]::NewGuid().ToString() }

function Write-JsonFile {
    param([string]$Path, [object]$Content)
    $dir = Split-Path $Path -Parent
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $json = $Content | ConvertTo-Json -Depth 30
    $absPath = Join-Path (Resolve-Path $dir).Path (Split-Path $Path -Leaf)
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($absPath, $json, $utf8NoBom)
}

function New-ColumnDef {
    param([string]$Name, [string]$DataType = "string", [string]$SummarizeBy = "none",
          [string]$Format = $null, [bool]$IsKey = $false, [bool]$IsHidden = $false)
    $col = [ordered]@{
        name = $Name; dataType = $DataType; sourceColumn = $Name
        summarizeBy = $SummarizeBy; lineageTag = (New-Guid)
    }
    if ($IsKey) { $col.isKey = $true }
    if ($IsHidden) { $col.isHidden = $true }
    if ($Format) { $col.formatString = $Format }
    $col
}

function New-CalcColumnDef {
    param([string]$Name, [string]$Expression, [string]$DataType = "string")
    [ordered]@{
        name = $Name; dataType = $DataType; type = "calculated"
        expression = $Expression; lineageTag = (New-Guid)
    }
}

function New-MeasureDef {
    param([string]$Name, [string]$Expression, [string]$Format = "#,##0", [string]$Folder = "Metrics")
    [ordered]@{
        name = $Name; expression = $Expression; formatString = $Format
        displayFolder = $Folder; lineageTag = (New-Guid)
    }
}

function New-CsvPartition {
    param([string]$TableName, [hashtable[]]$TypeMappings, [string[]]$PreTransformSteps)
    $typeLines = ($TypeMappings | ForEach-Object {
        "        {`"$($_.Name)`", $($_.Type)}"
    }) -join ",`n"

    $mExpr = [System.Collections.Generic.List[string]]::new()
    $mExpr.Add("let")
    $mExpr.Add("    Source = Csv.Document(File.Contents(CsvFolderPath & `"$TableName.csv`"), [Delimiter=`",`", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),")
    $mExpr.Add("    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),")

    $typedInput = "Headers"
    if ($PreTransformSteps -and $PreTransformSteps.Count -gt 0) {
        foreach ($step in $PreTransformSteps) { $mExpr.Add($step) }
        # Last pre-transform step name is extracted from the step text (e.g. "    StepName = ..." -> "StepName")
        $lastStep = $PreTransformSteps[-1] -replace '^\s+(\w+)\s*=.*', '$1'
        $typedInput = $lastStep
    }

    $mExpr.Add("    Typed = Table.TransformColumnTypes($typedInput, {")
    $mExpr.Add($typeLines)
    $mExpr.Add("    })")
    $mExpr.Add("in")
    $mExpr.Add("    Typed")

    [ordered]@{
        name = "$TableName-Partition"
        mode = "import"
        source = [ordered]@{ type = "m"; expression = [string[]]$mExpr }
    }
}

function New-RelationshipDef {
    param([string]$Name, [string]$FromTable, [string]$FromColumn, [string]$ToTable, [string]$ToColumn)
    [ordered]@{
        name = $Name; fromTable = $FromTable; fromColumn = $FromColumn
        toTable = $ToTable; toColumn = $ToColumn
        crossFilteringBehavior = "oneDirection"
        fromCardinality = "many"; toCardinality = "one"; isActive = $true
    }
}

# Visual builder helpers
function New-SourceRef { param([string]$Entity) @{ SourceRef = @{ Entity = $Entity } } }

function New-ColField {
    param([string]$Entity, [string]$Property)
    @{ Column = @{ Expression = (New-SourceRef $Entity); Property = $Property } }
}

function New-MeasureField {
    param([string]$Entity, [string]$Property)
    @{ Measure = @{ Expression = (New-SourceRef $Entity); Property = $Property } }
}

function New-Projection {
    param([string]$Table, [string]$Property, [string]$Type = "Column")
    $field = if ($Type -eq "Measure") { New-MeasureField $Table $Property } else { New-ColField $Table $Property }
    @{ field = $field; queryRef = "$Table.$Property" }
}

function New-CardVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Measure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "card"
            query = [ordered]@{
                queryState = [ordered]@{
                    Values = @{ projections = @(,(New-Projection $Table $Measure "Measure")) }
                }
            }
            objects = @{
                categoryLabels = @(@{ properties = @{ show = @{ expr = @{ Literal = @{ Value = "false" } } } } })
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-BarChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$CategoryCol, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "clusteredBarChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $Table $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $Table $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-DonutVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$CategoryCol, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "donutChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $Table $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $Table $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-TableVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string[]]$Columns, [string]$Title = $null)
    $projections = $Columns | ForEach-Object { New-Projection $Table $_ "Column" }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "tableEx"
            query = [ordered]@{
                queryState = [ordered]@{
                    Values = @{ projections = @($projections) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

# ============================================================================
# NEW VISUAL HELPERS
# ============================================================================

function New-ColumnChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "clusteredColumnChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $CatTable $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $ValTable $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-LineChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "lineChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $CatTable $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $ValTable $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-TreemapVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$GroupTable, [string]$GroupCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "treemap"
            query = [ordered]@{
                queryState = [ordered]@{
                    Group = @{ projections = @(,(New-Projection $GroupTable $GroupCol "Column")) }
                    Values = @{ projections = @(,(New-Projection $ValTable $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-GaugeVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "gauge"
            query = [ordered]@{
                queryState = [ordered]@{
                    Y = @{ projections = @(,(New-Projection $Table $ValueMeasure "Measure")) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-SlicerVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Column, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "slicer"
            query = [ordered]@{
                queryState = [ordered]@{
                    Values = @{ projections = @(,(New-Projection $Table $Column "Column")) }
                }
            }
            objects = @{
                data = @(@{ properties = @{
                    mode = @{ expr = @{ Literal = @{ Value = "'Dropdown'" } } }
                } })
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

function New-MatrixVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [hashtable[]]$RowFields, [hashtable[]]$ValueFields, [string]$Title = $null)
    $rowProjections = $RowFields | ForEach-Object { New-Projection $_.Table $_.Column "Column" }
    $valProjections = $ValueFields | ForEach-Object { New-Projection $_.Table $_.Measure "Measure" }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "pivotTable"
            query = [ordered]@{
                queryState = [ordered]@{
                    Rows = @{ projections = @($rowProjections) }
                    Values = @{ projections = @($valProjections) }
                }
            }
        }
    }
    if ($Title) {
        $vis.visual.visualContainerObjects = @{
            title = @(@{ properties = @{
                show = @{ expr = @{ Literal = @{ Value = "true" } } }
                text = @{ expr = @{ Literal = @{ Value = "'$Title'" } } }
            } })
        }
    }
    $vis
}

# ============================================================================
# PROJECT STRUCTURE
# ============================================================================

$projectName = "PowerPlatformGovernance2"
$reportDir = "$OutputPath/$projectName.Report"
$modelDir = "$OutputPath/$projectName.SemanticModel"

Write-Host "Cleaning old output..." -ForegroundColor Yellow
# Resolve output path — create if it doesn't exist
if (-not (Test-Path $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
    Write-Host "  Created output directory: $OutputPath" -ForegroundColor DarkGray
}
$OutputPath = (Resolve-Path $OutputPath).Path
try {
    Get-ChildItem -Path $OutputPath -Directory -Filter "*.Report" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $OutputPath -Directory -Filter "*.SemanticModel" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $OutputPath -Directory -Filter ".pbi" -Recurse -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
} catch {
    Write-Host "  Warning: Could not fully clean old output — $($_.Exception.Message)" -ForegroundColor DarkYellow
}

Write-Host "Creating PBIP project at: $OutputPath" -ForegroundColor Cyan

# Root .pbip file
Write-JsonFile "$OutputPath/$projectName.pbip" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json"
    version = "1.0"
    artifacts = @(@{ report = @{ path = "$projectName.Report" } })
    settings = @{ enableAutoRecovery = $true }
})

Set-Content "$OutputPath/.gitignore" @"
**/.pbi/localSettings.json
**/.pbi/cache.abf
"@ -Encoding UTF8

# ============================================================================
# SEMANTIC MODEL — TABLE DEFINITIONS WITH GOVERNANCE MEASURES
# ============================================================================

Write-Host "Building semantic model..." -ForegroundColor Yellow

$tEnvironments = [ordered]@{
    name = "Environments"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "EnvironmentId" "string" "none" -IsKey $true)
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "EnvironmentType")
        (New-ColumnDef "Region")
        (New-ColumnDef "State")
        (New-ColumnDef "IsDefault" "boolean")
        (New-ColumnDef "SecurityGroupId")
        (New-ColumnDef "OrgUrl")
        (New-ColumnDef "IsDataverseEnabled" "boolean")
        (New-ColumnDef "DatabaseUsedMb" "double" "sum" "#,##0.00")
        (New-ColumnDef "FileUsedMb" "double" "sum" "#,##0.00")
        (New-ColumnDef "LogUsedMb" "double" "sum" "#,##0.00")
        (New-ColumnDef "CreatedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "LastModifiedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "Environments" @(
        @{Name="EnvironmentId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="EnvironmentType"; Type="type text"}, @{Name="Region"; Type="type text"},
        @{Name="State"; Type="type text"}, @{Name="IsDefault"; Type="type logical"},
        @{Name="SecurityGroupId"; Type="type text"}, @{Name="OrgUrl"; Type="type text"},
        @{Name="IsDataverseEnabled"; Type="type logical"},
        @{Name="DatabaseUsedMb"; Type="type number"}, @{Name="FileUsedMb"; Type="type number"},
        @{Name="LogUsedMb"; Type="type number"},
        @{Name="CreatedTime"; Type="type datetime"}, @{Name="LastModifiedTime"; Type="type datetime"},
        @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Environments" "COUNTROWS('Environments')")
        (New-MeasureDef "Production Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[EnvironmentType] = `"Production`")")
        (New-MeasureDef "Sandbox Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[EnvironmentType] = `"Sandbox`")")
        (New-MeasureDef "Dataverse Enabled" "CALCULATE(COUNTROWS('Environments'), 'Environments'[IsDataverseEnabled] = TRUE())")
        (New-MeasureDef "Total Database MB" "SUM('Environments'[DatabaseUsedMb])" "#,##0.0" "Capacity")
        (New-MeasureDef "Total File MB" "SUM('Environments'[FileUsedMb])" "#,##0.0" "Capacity")
        (New-MeasureDef "Total Capacity GB" "DIVIDE(SUM('Environments'[DatabaseUsedMb]) + SUM('Environments'[FileUsedMb]) + SUM('Environments'[LogUsedMb]), 1024, 0)" "#,##0.00" "Capacity")
        # New governance measures
        (New-MeasureDef "Unsecured Environments" "CALCULATE(COUNTROWS('Environments'), ISBLANK('Environments'[SecurityGroupId]) || 'Environments'[SecurityGroupId] = `"`") + 0")
        (New-MeasureDef "Env Security Rate" "DIVIDE([Total Environments] - [Unsecured Environments], [Total Environments], 0)" "0.0%")
        (New-MeasureDef "Default Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[IsDefault] = TRUE())")
        (New-MeasureDef "Developer Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[EnvironmentType] = `"Developer`")")
        # Cross-table composite measures (hub table)
        (New-MeasureDef "Governance Score" "VAR SecurityScore = [Env Security Rate] * 20 VAR DlpScore = [DLP Coverage Rate] * 20 VAR FlowHealthScore = (1 - [Suspension Rate]) * 20 VAR SolutionScore = DIVIDE([Solution-Aware Flows] + [Solution-Aware Apps], [Total Flows] + [Total Apps], 0) * 20 VAR LifecycleScore = (1 - DIVIDE([Stale Apps (90d)] + [Stale Flows (90d)], [Total Apps] + [Total Flows], 0)) * 20 RETURN ROUND(SecurityScore + DlpScore + FlowHealthScore + SolutionScore + LifecycleScore, 0)" "0" "Governance")
        (New-MeasureDef "Flow Health Rate" "1 - [Suspension Rate]" "0.0%" "Governance")
        (New-MeasureDef "Shadow IT Rate" "DIVIDE([Unmanaged Flows] + CALCULATE(COUNTROWS('Apps'), 'Apps'[IsSolutionAware] = FALSE()), [Total Flows] + [Total Apps], 0)" "0.0%" "Risk")
        (New-MeasureDef "Total Resources" "[Total Apps] + [Total Flows] + [Total Agents]" "#,##0" "Metrics")
        (New-MeasureDef "Overall Solution Coverage" "DIVIDE([Solution-Aware Apps] + [Solution-Aware Flows], [Total Apps] + [Total Flows], 0)" "0.0%" "Governance")
        (New-MeasureDef "Security Coverage" "[Env Security Rate]" "0.0%" "Governance")
    )
}

$tApps = [ordered]@{
    name = "Apps"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "AppId" "string" "none" -IsKey $true)
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "Description")
        (New-ColumnDef "AppType")
        (New-ColumnDef "OwnerObjectId")
        (New-ColumnDef "OwnerDisplayName")
        (New-ColumnDef "OwnerEmail")
        (New-ColumnDef "CreatedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "LastModifiedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "LastPublishedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "AppVersion")
        (New-ColumnDef "Status")
        (New-ColumnDef "UsesPremiumApi" "boolean")
        (New-ColumnDef "UsesCustomApi" "boolean")
        (New-ColumnDef "SharedUsersCount" "int64" "sum" "#,##0")
        (New-ColumnDef "SharedGroupsCount" "int64" "sum" "#,##0")
        (New-ColumnDef "IsSolutionAware" "boolean")
        (New-ColumnDef "SolutionId")
        (New-ColumnDef "BypassConsent" "boolean")
        (New-ColumnDef "CollectedAt" "dateTime")
        # Calculated column for lifecycle page
        (New-CalcColumnDef "StalenessStatus" "IF('Apps'[LastModifiedTime] < TODAY() - 90, `"Stale`", `"Active`")")
    )
    partitions = @((New-CsvPartition "Apps" @(
        @{Name="AppId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="AppType"; Type="type text"},
        @{Name="OwnerObjectId"; Type="type text"}, @{Name="OwnerDisplayName"; Type="type text"},
        @{Name="OwnerEmail"; Type="type text"}, @{Name="CreatedTime"; Type="type datetime"},
        @{Name="LastModifiedTime"; Type="type datetime"}, @{Name="LastPublishedTime"; Type="type datetime"},
        @{Name="AppVersion"; Type="type text"}, @{Name="Status"; Type="type text"},
        @{Name="UsesPremiumApi"; Type="type logical"}, @{Name="UsesCustomApi"; Type="type logical"},
        @{Name="SharedUsersCount"; Type="Int64.Type"}, @{Name="SharedGroupsCount"; Type="Int64.Type"},
        @{Name="IsSolutionAware"; Type="type logical"}, @{Name="SolutionId"; Type="type text"},
        @{Name="BypassConsent"; Type="type logical"}, @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Apps" "COUNTROWS('Apps')")
        (New-MeasureDef "Canvas Apps" "CALCULATE(COUNTROWS('Apps'), CONTAINSSTRING('Apps'[AppType], `"Canvas`"))")
        (New-MeasureDef "Model-Driven Apps" "CALCULATE(COUNTROWS('Apps'), CONTAINSSTRING('Apps'[AppType], `"Model`"))")
        (New-MeasureDef "Premium API Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[UsesPremiumApi] = TRUE())")
        (New-MeasureDef "Solution-Aware Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[IsSolutionAware] = TRUE())")
        (New-MeasureDef "Total Shared Users" "SUM('Apps'[SharedUsersCount])")
        (New-MeasureDef "Avg Shared Users" "AVERAGE('Apps'[SharedUsersCount])" "#,##0.0")
        # New governance measures
        (New-MeasureDef "Stale Apps (90d)" "CALCULATE(COUNTROWS('Apps'), 'Apps'[LastModifiedTime] < TODAY() - 90)")
        (New-MeasureDef "Stale App Rate" "DIVIDE([Stale Apps (90d)], [Total Apps], 0)" "0.0%")
        (New-MeasureDef "Orphaned Apps" "CALCULATE(COUNTROWS('Apps'), ISBLANK('Apps'[OwnerEmail]) || 'Apps'[OwnerEmail] = `"`") + 0")
        (New-MeasureDef "Shared Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[SharedUsersCount] > 0)")
        (New-MeasureDef "Widely Shared Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[SharedUsersCount] >= 10)")
        (New-MeasureDef "Bypass Consent Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[BypassConsent] = TRUE())")
        (New-MeasureDef "Apps Created Last 30d" "CALCULATE(COUNTROWS('Apps'), 'Apps'[CreatedTime] >= TODAY() - 30)")
        (New-MeasureDef "Custom API Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[UsesCustomApi] = TRUE())")
        (New-MeasureDef "Unique App Owners" "DISTINCTCOUNT('Apps'[OwnerObjectId])" "#,##0" "Makers")
        (New-MeasureDef "Avg Apps Per Owner" "DIVIDE([Total Apps], [Unique App Owners], 0)" "0.0" "Makers")
    )
}

$tFlows = [ordered]@{
    name = "Flows"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId")
        (New-ColumnDef "FlowKey" "string" "none" -IsKey $true)
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "Description")
        (New-ColumnDef "State")
        (New-ColumnDef "CreatorObjectId")
        (New-ColumnDef "CreatorDisplayName")
        (New-ColumnDef "CreatedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "LastModifiedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "TriggerType")
        (New-ColumnDef "IsSolutionAware" "boolean")
        (New-ColumnDef "SolutionId")
        (New-ColumnDef "IsManaged" "boolean")
        (New-ColumnDef "SuspensionReason")
        (New-ColumnDef "CollectedAt" "dateTime")
        # Calculated column for lifecycle page
        (New-CalcColumnDef "ManagedStatus" "IF('Flows'[IsManaged] = TRUE(), `"Managed`", `"Unmanaged`")")
    )
    partitions = @((New-CsvPartition "Flows" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="State"; Type="type text"},
        @{Name="CreatorObjectId"; Type="type text"}, @{Name="CreatorDisplayName"; Type="type text"},
        @{Name="CreatedTime"; Type="type datetime"}, @{Name="LastModifiedTime"; Type="type datetime"},
        @{Name="TriggerType"; Type="type text"}, @{Name="IsSolutionAware"; Type="type logical"},
        @{Name="SolutionId"; Type="type text"}, @{Name="IsManaged"; Type="type logical"},
        @{Name="SuspensionReason"; Type="type text"}, @{Name="CollectedAt"; Type="type datetime"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
    )))
    measures = @(
        (New-MeasureDef "Total Flows" "COUNTROWS('Flows')")
        (New-MeasureDef "Active Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[State] = `"Started`")")
        (New-MeasureDef "Suspended Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[State] = `"Suspended`")")
        (New-MeasureDef "Stopped Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[State] = `"Stopped`")")
        (New-MeasureDef "Solution-Aware Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[IsSolutionAware] = TRUE())")
        (New-MeasureDef "Managed Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[IsManaged] = TRUE())")
        # New governance measures
        (New-MeasureDef "Suspension Rate" "DIVIDE([Suspended Flows], [Total Flows], 0)" "0.0%")
        (New-MeasureDef "Stale Flows (90d)" "CALCULATE(COUNTROWS('Flows'), 'Flows'[LastModifiedTime] < TODAY() - 90)")
        (New-MeasureDef "Stale Flow Rate" "DIVIDE([Stale Flows (90d)], [Total Flows], 0)" "0.0%")
        (New-MeasureDef "Scheduled Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[TriggerType] = `"Recurrence`")")
        (New-MeasureDef "Manual Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[TriggerType] = `"Request`")")
        (New-MeasureDef "Unmanaged Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[IsManaged] = FALSE())")
        (New-MeasureDef "Flows Created Last 30d" "CALCULATE(COUNTROWS('Flows'), 'Flows'[CreatedTime] >= TODAY() - 30)")
        (New-MeasureDef "Avg Actions Per Flow" "DIVIDE(COUNTROWS('FlowActions'), COUNTROWS('Flows'), 0)" "#,##0.0")
        (New-MeasureDef "Orphaned Flows" "CALCULATE(COUNTROWS('Flows'), ISBLANK('Flows'[CreatorDisplayName]) || 'Flows'[CreatorDisplayName] = `"`") + 0" "#,##0" "Risk")
        (New-MeasureDef "Unique Flow Creators" "DISTINCTCOUNT('Flows'[CreatorObjectId])" "#,##0" "Makers")
        (New-MeasureDef "Avg Flows Per Creator" "DIVIDE([Total Flows], [Unique Flow Creators], 0)" "0.0" "Makers")
    )
}

$tConnectors = [ordered]@{
    name = "Connectors"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "Description")
        (New-ColumnDef "Publisher")
        (New-ColumnDef "Tier")
        (New-ColumnDef "IsCustom" "boolean")
        (New-ColumnDef "IconUri")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "Connectors" @(
        @{Name="ConnectorId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="Publisher"; Type="type text"},
        @{Name="Tier"; Type="type text"}, @{Name="IsCustom"; Type="type logical"},
        @{Name="IconUri"; Type="type text"}, @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Connectors" "COUNTROWS('Connectors')")
        (New-MeasureDef "Custom Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[IsCustom] = TRUE())")
        (New-MeasureDef "Premium Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[Tier] = `"Premium`") + 0")
        # New governance measures
        (New-MeasureDef "Standard Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[Tier] = `"Standard`")")
        (New-MeasureDef "Unique Connector Types" "DISTINCTCOUNT('Connectors'[DisplayName])")
        (New-MeasureDef "Connector Utilization" "DIVIDE(DISTINCTCOUNT('FlowConnectionRefs'[ConnectorId]), COUNTROWS('Connectors'), 0)" "0.0%")
    )
}

$tDlpPolicies = [ordered]@{
    name = "DlpPolicies"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "PolicyId" "string" "none" -IsKey $true)
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "Description")
        (New-ColumnDef "IsEnabled" "boolean")
        (New-ColumnDef "PolicyType")
        (New-ColumnDef "EnvironmentScope")
        (New-ColumnDef "CreatedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "LastModifiedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "DlpPolicies" @(
        @{Name="PolicyId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="IsEnabled"; Type="type logical"},
        @{Name="PolicyType"; Type="type text"}, @{Name="EnvironmentScope"; Type="type text"},
        @{Name="CreatedTime"; Type="type datetime"}, @{Name="LastModifiedTime"; Type="type datetime"},
        @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total DLP Policies" "COUNTROWS('DlpPolicies')")
        (New-MeasureDef "Enabled Policies" "CALCULATE(COUNTROWS('DlpPolicies'), 'DlpPolicies'[IsEnabled] = TRUE())")
        (New-MeasureDef "DLP Coverage Rate" "DIVIDE([Enabled Policies], [Total DLP Policies], 0)" "0.0%" "Governance")
    )
}

$tDlpRules = [ordered]@{
    name = "DlpConnectorRules"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "PolicyId" "string" "none" $null $false $true)
        (New-ColumnDef "PolicyName")
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "ConnectorName")
        (New-ColumnDef "Classification")
    )
    partitions = @((New-CsvPartition "DlpConnectorRules" @(
        @{Name="PolicyId"; Type="type text"}, @{Name="PolicyName"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="ConnectorName"; Type="type text"},
        @{Name="Classification"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Connector Rules" "COUNTROWS('DlpConnectorRules')" "#,##0" "DLP")
        (New-MeasureDef "Business Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"Business`") + 0" "#,##0" "DLP")
        (New-MeasureDef "Non-Business Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"NonBusiness`") + 0" "#,##0" "DLP")
        (New-MeasureDef "Blocked Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"Blocked`")" "#,##0" "DLP")
        (New-MeasureDef "Blocked Connector Rate" "DIVIDE([Blocked Connectors], COUNTROWS('DlpConnectorRules'), 0)" "0.0%" "DLP")
    )
}

$tUsage = [ordered]@{
    name = "UsageAnalytics"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ResourceType")
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "Date" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "UniqueUsers" "int64" "sum" "#,##0")
        (New-ColumnDef "TotalSessions" "int64" "sum" "#,##0")
        (New-ColumnDef "TotalActions" "int64" "sum" "#,##0")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "UsageAnalytics" @(
        @{Name="ResourceType"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Date"; Type="type datetime"}, @{Name="UniqueUsers"; Type="Int64.Type"},
        @{Name="TotalSessions"; Type="Int64.Type"}, @{Name="TotalActions"; Type="Int64.Type"},
        @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Unique Users" "SUM('UsageAnalytics'[UniqueUsers])" "#,##0" "Usage")
        (New-MeasureDef "Total Sessions" "SUM('UsageAnalytics'[TotalSessions])" "#,##0" "Usage")
        (New-MeasureDef "Total Actions" "SUM('UsageAnalytics'[TotalActions])" "#,##0" "Usage")
    )
}

$tAppConnRefs = [ordered]@{
    name = "AppConnectorRefs"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "AppId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "DataSources")
        (New-ColumnDef "EndpointUrl")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
    )
    partitions = @((New-CsvPartition "AppConnectorRefs" @(
        @{Name="AppId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="DataSources"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Connector References" "COUNTROWS('AppConnectorRefs')")
        (New-MeasureDef "Distinct App Endpoints" "DISTINCTCOUNT('AppConnectorRefs'[EndpointUrl])")
        (New-MeasureDef "App Refs with Endpoints" "CALCULATE(COUNTROWS('AppConnectorRefs'), NOT(ISBLANK('AppConnectorRefs'[EndpointUrl])) && 'AppConnectorRefs'[EndpointUrl] <> `"`") + 0")
        (New-MeasureDef "App HTTP Connector Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), NOT(ISBLANK('AppConnectorRefs'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "App HTTP Raw Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), 'AppConnectorRefs'[HttpConnectorType] = `"HTTP`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "App HTTP Entra Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), 'AppConnectorRefs'[HttpConnectorType] = `"HTTP with Azure AD`") + 0" "#,##0" "HTTP Risk")
    )
}

$tFlowActions = [ordered]@{
    name = "FlowActions"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId" "string" "none" $null $false $true)
        (New-ColumnDef "FlowKey" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "Position" "int64" "none")
        (New-ColumnDef "Name")
        (New-ColumnDef "ActionType")
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "OperationId")
        (New-ColumnDef "EndpointUrl")
        (New-ColumnDef "BaseUrl")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('FlowActions'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('FlowActions'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowActions'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowActions'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
    )
    partitions = @((New-CsvPartition "FlowActions" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="ActionType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
        '    EnsureBaseUrl = if Table.HasColumns(EnsureFlowKey, "BaseUrl") then EnsureFlowKey else Table.AddColumn(EnsureFlowKey, "BaseUrl", each ""),'
    )))
    measures = @(
        (New-MeasureDef "Total Flow Actions" "COUNTROWS('FlowActions')")
        (New-MeasureDef "Distinct Action Endpoints" "DISTINCTCOUNT('FlowActions'[EndpointUrl])")
        (New-MeasureDef "Actions with Endpoints" "CALCULATE(COUNTROWS('FlowActions'), NOT(ISBLANK('FlowActions'[EndpointUrl])) && 'FlowActions'[EndpointUrl] <> `"`") + 0")
        (New-MeasureDef "Flow HTTP Actions" "CALCULATE(COUNTROWS('FlowActions'), NOT(ISBLANK('FlowActions'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow HTTP Raw Actions" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[HttpConnectorType] = `"HTTP`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow HTTP Entra Actions" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[HttpConnectorType] = `"HTTP with Azure AD`") + 0" "#,##0" "HTTP Risk")
    )
}

$tFlowTriggers = [ordered]@{
    name = "FlowTriggers"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId" "string" "none" $null $false $true)
        (New-ColumnDef "FlowKey" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "Position" "int64" "none")
        (New-ColumnDef "Name")
        (New-ColumnDef "TriggerType")
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "OperationId")
        (New-ColumnDef "EndpointUrl")
        (New-ColumnDef "BaseUrl")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('FlowTriggers'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
    )
    partitions = @((New-CsvPartition "FlowTriggers" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="TriggerType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
        '    EnsureBaseUrl = if Table.HasColumns(EnsureFlowKey, "BaseUrl") then EnsureFlowKey else Table.AddColumn(EnsureFlowKey, "BaseUrl", each ""),'
    )))
    measures = @(
        (New-MeasureDef "Total Flow Triggers" "COUNTROWS('FlowTriggers')")
        (New-MeasureDef "Distinct Trigger Endpoints" "DISTINCTCOUNT('FlowTriggers'[EndpointUrl])")
        (New-MeasureDef "Flow HTTP Triggers" "CALCULATE(COUNTROWS('FlowTriggers'), NOT(ISBLANK('FlowTriggers'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
    )
}

$tFlowConnRefs = [ordered]@{
    name = "FlowConnectionRefs"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId" "string" "none" $null $false $true)
        (New-ColumnDef "FlowKey" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "ConnectionName")
        (New-ColumnDef "ConnectionUrl")
    )
    partitions = @((New-CsvPartition "FlowConnectionRefs" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="ConnectionName"; Type="type text"},
        @{Name="ConnectionUrl"; Type="type text"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
    )))
    measures = @(
        (New-MeasureDef "Total Flow Connections" "COUNTROWS('FlowConnectionRefs')")
        (New-MeasureDef "Distinct Connection URLs" "DISTINCTCOUNT('FlowConnectionRefs'[ConnectionUrl])")
        (New-MeasureDef "Flow Connection Count" "COUNTROWS('FlowConnectionRefs')" "#,##0" "Connections")
    )
}

# --- Table 12: CopilotAgents ---

$tCopilotAgents = [ordered]@{
    name = "CopilotAgents"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "AgentKey" "string" "none" $null $true $true)
        (New-ColumnDef "BotId" "string" "none" $null $false)
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "SchemaName")
        (New-ColumnDef "AgentType")
        (New-ColumnDef "Language")
        (New-ColumnDef "AuthenticationMode")
        (New-ColumnDef "AuthenticationTrigger")
        (New-ColumnDef "AccessControlPolicy")
        (New-ColumnDef "RuntimeProvider")
        (New-ColumnDef "SupportedLanguages")
        (New-ColumnDef "State")
        (New-ColumnDef "StatusReason")
        (New-ColumnDef "PublishedOn" "dateTime")
        (New-ColumnDef "PublishedByName")
        (New-ColumnDef "Origin")
        (New-ColumnDef "Template")
        (New-ColumnDef "IsManaged" "boolean")
        (New-ColumnDef "SolutionId")
        (New-ColumnDef "Configuration")
        (New-ColumnDef "CreatedOn" "dateTime")
        (New-ColumnDef "CreatedByName")
        (New-ColumnDef "ModifiedOn" "dateTime")
        (New-ColumnDef "ModifiedByName")
        (New-ColumnDef "TopicCount" "int64" "none")
        (New-ColumnDef "KnowledgeSourceCount" "int64" "none")
        (New-ColumnDef "SkillCount" "int64" "none")
        (New-ColumnDef "CustomGPTCount" "int64" "none")
        (New-ColumnDef "TotalComponents" "int64" "none")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "CopilotAgents" @(
        @{Name="AgentKey"; Type="type text"},
        @{Name="BotId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="SchemaName"; Type="type text"}, @{Name="AgentType"; Type="type text"},
        @{Name="Language"; Type="type text"}, @{Name="AuthenticationMode"; Type="type text"},
        @{Name="AuthenticationTrigger"; Type="type text"}, @{Name="AccessControlPolicy"; Type="type text"},
        @{Name="RuntimeProvider"; Type="type text"}, @{Name="SupportedLanguages"; Type="type text"},
        @{Name="State"; Type="type text"}, @{Name="StatusReason"; Type="type text"},
        @{Name="PublishedOn"; Type="type datetime"}, @{Name="PublishedByName"; Type="type text"},
        @{Name="Origin"; Type="type text"}, @{Name="Template"; Type="type text"},
        @{Name="IsManaged"; Type="type logical"}, @{Name="SolutionId"; Type="type text"},
        @{Name="Configuration"; Type="type text"},
        @{Name="CreatedOn"; Type="type datetime"}, @{Name="CreatedByName"; Type="type text"},
        @{Name="ModifiedOn"; Type="type datetime"}, @{Name="ModifiedByName"; Type="type text"},
        @{Name="TopicCount"; Type="Int64.Type"}, @{Name="KnowledgeSourceCount"; Type="Int64.Type"},
        @{Name="SkillCount"; Type="Int64.Type"}, @{Name="CustomGPTCount"; Type="Int64.Type"},
        @{Name="TotalComponents"; Type="Int64.Type"}, @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Agents" "COUNTROWS('CopilotAgents')" "#,##0" "Agent Metrics")
        (New-MeasureDef "Declarative Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[AgentType] = `"Declarative`")" "#,##0" "Agent Metrics")
        (New-MeasureDef "Custom Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[AgentType] = `"Custom`")" "#,##0" "Agent Metrics")
        (New-MeasureDef "Active Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[State] = `"Active`")" "#,##0" "Agent Metrics")
        (New-MeasureDef "Inactive Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[State] = `"Inactive`")" "#,##0" "Agent Metrics")
        (New-MeasureDef "Published Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[StatusReason] = `"Provisioned`")" "#,##0" "Agent Metrics")
        (New-MeasureDef "Managed Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[IsManaged] = TRUE())" "#,##0" "Agent Metrics")
        (New-MeasureDef "Total Topics" "SUM('CopilotAgents'[TopicCount])" "#,##0" "Component Metrics")
        (New-MeasureDef "Total Knowledge Sources" "SUM('CopilotAgents'[KnowledgeSourceCount])" "#,##0" "Component Metrics")
        (New-MeasureDef "Total Skills" "SUM('CopilotAgents'[SkillCount])" "#,##0" "Component Metrics")
        (New-MeasureDef "Total Agent Components" "SUM('CopilotAgents'[TotalComponents])" "#,##0" "Component Metrics")
        (New-MeasureDef "Avg Components Per Agent" "DIVIDE(SUM('CopilotAgents'[TotalComponents]), COUNTROWS('CopilotAgents'), 0)" "0.0" "Component Metrics")
        (New-MeasureDef "Unique Agent Creators" "DISTINCTCOUNT('CopilotAgents'[CreatedByName])" "#,##0" "Agent Metrics")
        (New-MeasureDef "Agent Active Rate" "DIVIDE([Active Agents], [Total Agents], 0)" "0.0%" "Agent Metrics")
    )
}

# --- Table 13: CopilotComponents ---

$tCopilotComponents = [ordered]@{
    name = "CopilotComponents"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ComponentId" "string" "none" $null $false)
        (New-ColumnDef "AgentKey" "string" "none" $null $false $true)
        (New-ColumnDef "BotId" "string" "none" $null $false $true)
        (New-ColumnDef "BotName")
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "Name")
        (New-ColumnDef "ComponentType")
        (New-ColumnDef "Category")
        (New-ColumnDef "Description")
        (New-ColumnDef "Status")
        (New-ColumnDef "IsManaged" "boolean")
        (New-ColumnDef "CreatedOn" "dateTime")
        (New-ColumnDef "ModifiedOn" "dateTime")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "CopilotComponents" @(
        @{Name="ComponentId"; Type="type text"}, @{Name="AgentKey"; Type="type text"},
        @{Name="BotId"; Type="type text"},
        @{Name="BotName"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="Name"; Type="type text"},
        @{Name="ComponentType"; Type="type text"}, @{Name="Category"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="Status"; Type="type text"},
        @{Name="IsManaged"; Type="type logical"},
        @{Name="CreatedOn"; Type="type datetime"}, @{Name="ModifiedOn"; Type="type datetime"},
        @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Components" "COUNTROWS('CopilotComponents')" "#,##0" "Components")
        (New-MeasureDef "Topic Components" "CALCULATE(COUNTROWS('CopilotComponents'), 'CopilotComponents'[ComponentType] IN {`"Topic`", `"Topic V2`"})" "#,##0" "Components")
        (New-MeasureDef "Skill Components" "CALCULATE(COUNTROWS('CopilotComponents'), 'CopilotComponents'[ComponentType] IN {`"Skill`", `"Skill V2`"})" "#,##0" "Components")
        (New-MeasureDef "Knowledge Source Components" "CALCULATE(COUNTROWS('CopilotComponents'), 'CopilotComponents'[ComponentType] = `"Knowledge Source`")" "#,##0" "Components")
        (New-MeasureDef "Custom GPT Components" "CALCULATE(COUNTROWS('CopilotComponents'), 'CopilotComponents'[ComponentType] = `"Custom GPT`")" "#,##0" "Components")
        (New-MeasureDef "Distinct Component Types" "DISTINCTCOUNT('CopilotComponents'[ComponentType])" "#,##0" "Components")
    )
}

# --- Table 14: Connections ---

$tConnections = [ordered]@{
    name = "Connections"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ConnectionId" "string" "none" -IsKey $true)
        (New-ColumnDef "ConnectorId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentId" "string" "none" $null $false $true)
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "DisplayName")
        (New-ColumnDef "ConnectionUrl")
        (New-ColumnDef "CreatedByObjectId")
        (New-ColumnDef "CreatedByName")
        (New-ColumnDef "CreatedByEmail")
        (New-ColumnDef "CreatedTime" "dateTime" "none" "yyyy-MM-dd")
        (New-ColumnDef "Status")
        (New-ColumnDef "IsShared" "boolean")
        (New-ColumnDef "CollectedAt" "dateTime")
    )
    partitions = @((New-CsvPartition "Connections" @(
        @{Name="ConnectionId"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"}, @{Name="EnvironmentName"; Type="type text"},
        @{Name="DisplayName"; Type="type text"}, @{Name="ConnectionUrl"; Type="type text"},
        @{Name="CreatedByObjectId"; Type="type text"}, @{Name="CreatedByName"; Type="type text"},
        @{Name="CreatedByEmail"; Type="type text"}, @{Name="CreatedTime"; Type="type datetime"},
        @{Name="Status"; Type="type text"}, @{Name="IsShared"; Type="type logical"},
        @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Connections" "COUNTROWS('Connections')" "#,##0" "Connections")
        (New-MeasureDef "Shared Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[IsShared] = TRUE())" "#,##0" "Connections")
        (New-MeasureDef "Active Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[Status] = `"Connected`")" "#,##0" "Connections")
        (New-MeasureDef "Unique Connection Creators" "DISTINCTCOUNT('Connections'[CreatedByObjectId])" "#,##0" "Connections")
        (New-MeasureDef "Connector Types Used" "DISTINCTCOUNT('Connections'[ConnectorId])" "#,##0" "Connections")
    )
}

# --- Build model.bim ---

$modelBim = [ordered]@{
    compatibilityLevel = 1567
    model = [ordered]@{
        culture = "en-US"
        defaultPowerBIDataSourceVersion = "powerBI_V3"
        sourceQueryCulture = "en-US"
        tables = @($tEnvironments, $tApps, $tFlows, $tConnectors,
                    $tDlpPolicies, $tDlpRules, $tUsage, $tAppConnRefs, $tFlowActions, $tFlowTriggers, $tFlowConnRefs,
                    $tCopilotAgents, $tCopilotComponents, $tConnections)
        relationships = @(
            (New-RelationshipDef "rel_Apps_Env" "Apps" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Flows_Env" "Flows" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connectors_Env" "Connectors" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_DlpRules_Policy" "DlpConnectorRules" "PolicyId" "DlpPolicies" "PolicyId")
            (New-RelationshipDef "rel_Usage_Env" "UsageAnalytics" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_AppConnRefs_Apps" "AppConnectorRefs" "AppId" "Apps" "AppId")
            (New-RelationshipDef "rel_FlowActions_Flows" "FlowActions" "FlowKey" "Flows" "FlowKey")
            (New-RelationshipDef "rel_FlowTriggers_Flows" "FlowTriggers" "FlowKey" "Flows" "FlowKey")
            (New-RelationshipDef "rel_FlowConnRefs_Flows" "FlowConnectionRefs" "FlowKey" "Flows" "FlowKey")
            (New-RelationshipDef "rel_Agents_Env" "CopilotAgents" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Components_Agent" "CopilotComponents" "AgentKey" "CopilotAgents" "AgentKey")
            (New-RelationshipDef "rel_Connections_Env" "Connections" "EnvironmentId" "Environments" "EnvironmentId")
        )
        expressions = @(
            [ordered]@{
                name = "CsvFolderPath"
                kind = "m"
                expression = @("`"$($CsvPath -replace '\\', '\\')`" meta [IsParameterQuery=true, Type=`"Text`", IsParameterQueryRequired=true]")
            }
        )
        annotations = @(
            @{ name = "PBI_QueryOrder"; value = "[`"Environments`",`"Apps`",`"Flows`",`"Connectors`",`"DlpPolicies`",`"DlpConnectorRules`",`"UsageAnalytics`",`"AppConnectorRefs`",`"FlowActions`",`"FlowTriggers`",`"FlowConnectionRefs`",`"CopilotAgents`",`"CopilotComponents`",`"Connections`"]" }
            @{ name = "__PBI_TimeIntelligenceEnabled"; value = "0" }
        )
    }
}

Write-JsonFile "$modelDir/definition.pbism" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json"
    version = "4.0"
})
Write-JsonFile "$modelDir/model.bim" $modelBim

# ============================================================================
# REPORT DEFINITION (PBIR format)
# ============================================================================

Write-Host "Building report pages..." -ForegroundColor Yellow

Write-JsonFile "$reportDir/definition.pbir" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json"
    version = "4.0"
    datasetReference = @{ byPath = @{ path = "../$projectName.SemanticModel" } }
})

$defDir = "$reportDir/definition"
Write-JsonFile "$defDir/version.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json"
    version = "2.0.0"
})

$reportJsonContent = @'
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
  "layoutOptimization": "None",
  "themeCollection": {
    "baseTheme": {
      "name": "CY24SU10",
      "reportVersionAtImport": "5.55",
      "type": "SharedResources"
    }
  },
  "resourcePackages": [
    {
      "name": "SharedResources",
      "type": "SharedResources",
      "items": [
        {
          "name": "CY24SU10",
          "path": "BaseThemes/CY24SU10.json",
          "type": "BaseTheme"
        }
      ]
    }
  ],
  "settings": {
    "useStylableVisualContainerHeader": true,
    "defaultDrillFilterOtherVisuals": true,
    "useEnhancedTooltips": false
  },
  "slowDataSourceSettings": {
    "isCrossHighlightingDisabled": false,
    "isSlicerSelectionsButtonEnabled": false,
    "isFilterSelectionsButtonEnabled": false,
    "isFieldWellButtonEnabled": false,
    "isApplyAllButtonEnabled": false
  }
}
'@
$reportDir2 = Split-Path "$defDir/report.json" -Parent
if (-not (Test-Path $reportDir2)) { New-Item -ItemType Directory -Path $reportDir2 -Force | Out-Null }
$absReportJson = Join-Path (Resolve-Path $reportDir2).Path "report.json"
[System.IO.File]::WriteAllText($absReportJson, $reportJsonContent, [System.Text.UTF8Encoding]::new($false))

# ============================================================================
# 16 PAGES (1 command center + 6 domain + 3 risk/intelligence + 6 detail)
# ============================================================================

$pageNames = @("executive", "environments", "apps", "flows", "agents", "connectors-dlp", "endpoints", "risk", "makers", "connections", "env-details", "app-details", "flow-details", "agent-details", "dlp-details", "connector-details")
Write-JsonFile "$defDir/pages/pages.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json"
    pageOrder = $pageNames
    activePageName = "executive"
})

# --- Page 1: Executive Command Center ---
$execMatrix = New-MatrixVisual "matrixEnvMetrics" 20 380 1220 160 8000 `
    @(@{Table="Environments"; Column="DisplayName"}) `
    @(@{Table="Apps"; Measure="Total Apps"},
      @{Table="Flows"; Measure="Total Flows"},
      @{Table="CopilotAgents"; Measure="Total Agents"},
      @{Table="Flows"; Measure="Suspended Flows"},
      @{Table="Environments"; Measure="Total Capacity GB"}) `
    "Environment Matrix"

$pageDefs = @{
    executive = @{
        displayName = "Executive Command Center"
        visuals = @(
            # Full-width KPI banner
            (New-CardVisual "cardExecEnv" 20 20 120 80 100 "Environments" "Total Environments" "Environments")
            (New-CardVisual "cardExecApps" 155 20 120 80 200 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardExecFlows" 290 20 120 80 300 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardExecConn" 425 20 120 80 400 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardExecAgents" 560 20 120 80 500 "CopilotAgents" "Total Agents" "Agents")
            (New-CardVisual "cardExecDlp" 695 20 120 80 600 "DlpPolicies" "Total DLP Policies" "DLP Policies")
            # 3 gauges
            (New-GaugeVisual "gaugeGovScore" 20 120 280 240 1000 "Environments" "Governance Score" "Governance Score")
            (New-GaugeVisual "gaugeFlowHealth" 315 120 280 240 2000 "Environments" "Flow Health Rate" "Flow Health Rate")
            (New-GaugeVisual "gaugeSecCoverage" 610 120 280 240 3000 "Environments" "Security Coverage" "Security Coverage")
            # Treemap + Column chart
            (New-TreemapVisual "tmEnvResources" 905 120 355 240 5000 "Environments" "DisplayName" "Environments" "Total Resources" "Environments by Resources")
            # Environment matrix
            $execMatrix
            # Resource distribution
            (New-ColumnChartVisual "colResDist" 20 555 1220 155 6000 "Environments" "DisplayName" "Environments" "Total Resources" "Resource Distribution by Environment")
        )
    }
    environments = @{
        displayName = "Environment Health"
        visuals = @(
            # Cards row
            (New-CardVisual "cardEnvTotal" 20 20 120 80 100 "Environments" "Total Environments" "Total")
            (New-CardVisual "cardEnvProd" 155 20 120 80 200 "Environments" "Production Environments" "Production")
            (New-CardVisual "cardEnvSandbox" 290 20 120 80 300 "Environments" "Sandbox Environments" "Sandbox")
            (New-CardVisual "cardEnvDev" 425 20 120 80 350 "Environments" "Developer Environments" "Developer")
            (New-CardVisual "cardEnvDefault" 560 20 120 80 375 "Environments" "Default Environments" "Default")
            (New-CardVisual "cardEnvDataverse" 695 20 120 80 400 "Environments" "Dataverse Enabled" "Dataverse")
            # Chart row: donut + bar + gauge
            (New-DonutVisual "donutEnvType" 20 120 380 240 1000 "Environments" "EnvironmentType" "Total Environments" "Environment Types")
            (New-BarChartVisual "barCapacity" 415 120 430 240 2000 "Environments" "DisplayName" "Total Capacity GB" "Capacity by Environment")
            (New-GaugeVisual "gaugeEnvSec" 860 120 185 120 3000 "Environments" "Env Security Rate" "Security Rate")
            (New-CardVisual "cardEnvUnsecured" 860 255 185 105 3500 "Environments" "Unsecured Environments" "Unsecured Environments")
            # Detail table
            (New-TableVisual "tblEnvDetails" 20 380 1220 320 4000 "Environments" @("DisplayName","EnvironmentType","Region","State","IsDefault","IsDataverseEnabled","DatabaseUsedMb","FileUsedMb","LogUsedMb","SecurityGroupId","CreatedTime","LastModifiedTime") "Environment Details")
        )
    }
    apps = @{
        displayName = "App Portfolio"
        visuals = @(
            # Slicer + cards
            (New-SlicerVisual "slicerEnvApps" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardAppTotal" 210 20 120 80 100 "Apps" "Total Apps" "Total Apps")
            (New-CardVisual "cardAppCanvas" 345 20 120 80 200 "Apps" "Canvas Apps" "Canvas")
            (New-CardVisual "cardAppModel" 480 20 120 80 300 "Apps" "Model-Driven Apps" "Model-Driven")
            (New-CardVisual "cardAppPremium" 615 20 120 80 400 "Apps" "Premium API Apps" "Premium API")
            (New-CardVisual "cardAppStale" 750 20 120 80 500 "Apps" "Stale Apps (90d)" "Stale (90d)")
            (New-CardVisual "cardAppOrphan" 885 20 120 80 550 "Apps" "Orphaned Apps" "Orphaned")
            # Chart row 1: donut + bar
            (New-DonutVisual "donutAppType" 20 120 430 240 1000 "Apps" "AppType" "Total Apps" "App Types")
            (New-BarChartVisual "barAppOwners" 465 120 430 240 2000 "Apps" "OwnerDisplayName" "Total Apps" "Top App Owners")
            # Chart row 2: donut + bar
            (New-DonutVisual "donutAppSolution" 910 120 350 240 2500 "Apps" "IsSolutionAware" "Total Apps" "Solution Awareness")
            # Detail table
            (New-TableVisual "tblApps" 20 380 1220 320 4000 "Apps" @("DisplayName","AppType","OwnerDisplayName","OwnerEmail","UsesPremiumApi","SharedUsersCount","StalenessStatus","IsSolutionAware","EnvironmentName","LastModifiedTime") "App Details")
        )
    }
    flows = @{
        displayName = "Flow Operations"
        visuals = @(
            # Slicer + cards
            (New-SlicerVisual "slicerEnvFlows" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardFlowTotal" 210 20 120 80 100 "Flows" "Total Flows" "Total")
            (New-CardVisual "cardFlowActive" 345 20 120 80 200 "Flows" "Active Flows" "Active")
            (New-CardVisual "cardFlowSusp" 480 20 120 80 300 "Flows" "Suspended Flows" "Suspended")
            (New-CardVisual "cardFlowStopped" 615 20 120 80 400 "Flows" "Stopped Flows" "Stopped")
            (New-CardVisual "cardFlowSolution" 750 20 120 80 450 "Flows" "Solution-Aware Flows" "Solution-Aware")
            (New-CardVisual "cardFlowManaged" 885 20 120 80 500 "Flows" "Managed Flows" "Managed")
            # Chart row 1: donut + bar
            (New-DonutVisual "donutFlowState" 20 120 430 240 1000 "Flows" "State" "Total Flows" "Flow States")
            (New-BarChartVisual "barSuspReason" 465 120 430 240 2000 "Flows" "SuspensionReason" "Suspended Flows" "Suspension Reasons")
            # Chart row 2: donut + bar
            (New-DonutVisual "donutTrigger" 910 120 350 120 3000 "Flows" "TriggerType" "Total Flows" "Trigger Types")
            (New-BarChartVisual "barFlowCreators" 910 255 350 105 3500 "Flows" "CreatorDisplayName" "Total Flows" "Top Flow Creators")
            # Line chart: activity trend
            (New-LineChartVisual "lineFlowActivity" 20 380 600 160 4000 "Flows" "LastModifiedTime" "Flows" "Total Flows" "Flows by Last Modified Month")
            # Detail table
            (New-TableVisual "tblFlows" 20 555 1220 155 5000 "Flows" @("DisplayName","State","TriggerType","CreatorDisplayName","IsManaged","IsSolutionAware","SuspensionReason","EnvironmentName","LastModifiedTime") "Flow Details")
        )
    }
    agents = @{
        displayName = "Copilot Agents"
        visuals = @(
            # Slicer + cards
            (New-SlicerVisual "slicerEnvAgents" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardAgentTotal" 210 20 120 80 100 "CopilotAgents" "Total Agents" "Total Agents")
            (New-CardVisual "cardAgentDecl" 345 20 120 80 200 "CopilotAgents" "Declarative Agents" "Declarative")
            (New-CardVisual "cardAgentCustom" 480 20 120 80 300 "CopilotAgents" "Custom Agents" "Custom")
            (New-CardVisual "cardAgentActive" 615 20 120 80 400 "CopilotAgents" "Active Agents" "Active")
            (New-CardVisual "cardAgentPublished" 750 20 120 80 450 "CopilotAgents" "Published Agents" "Published")
            (New-CardVisual "cardAgentManaged" 885 20 120 80 500 "CopilotAgents" "Managed Agents" "Managed")
            # Chart row 1: donut + bar
            (New-DonutVisual "donutAgentType" 20 120 380 240 1000 "CopilotAgents" "AgentType" "Total Agents" "Agent Types")
            (New-BarChartVisual "barAgentEnv" 415 120 430 240 2000 "CopilotAgents" "EnvironmentName" "Total Agents" "Agents by Environment")
            # Chart row 2: donut + treemap
            (New-DonutVisual "donutAgentAuth" 860 120 185 120 3000 "CopilotAgents" "AuthenticationMode" "Total Agents" "Auth Modes")
            (New-TreemapVisual "tmCompTypes" 860 255 185 105 3500 "CopilotComponents" "ComponentType" "CopilotComponents" "Total Components" "Component Types")
            # Bar + component cards
            (New-BarChartVisual "barAgentCreators" 20 380 430 120 4000 "CopilotAgents" "CreatedByName" "Total Agents" "Top Agent Creators")
            (New-CardVisual "cardTopics" 465 380 145 55 4100 "CopilotAgents" "Total Topics" "Topics")
            (New-CardVisual "cardKnowledge" 625 380 145 55 4200 "CopilotAgents" "Total Knowledge Sources" "Knowledge Sources")
            (New-CardVisual "cardSkills" 785 380 145 55 4300 "CopilotAgents" "Total Skills" "Skills")
            # Detail table
            (New-TableVisual "tblAgents" 20 455 1220 255 5000 "CopilotAgents" @("DisplayName","AgentType","State","StatusReason","AuthenticationMode","EnvironmentName","TopicCount","KnowledgeSourceCount","SkillCount","PublishedOn","CreatedByName") "Agent Summary")
        )
    }
    "connectors-dlp" = @{
        displayName = "Connector & DLP Governance"
        visuals = @(
            # Cards row
            (New-CardVisual "cardConnTotal" 20 20 120 80 100 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardConnPrem" 155 20 120 80 200 "Connectors" "Premium Connectors" "Premium")
            (New-CardVisual "cardConnCustom" 290 20 120 80 300 "Connectors" "Custom Connectors" "Custom")
            (New-CardVisual "cardConnStd" 425 20 120 80 400 "Connectors" "Standard Connectors" "Standard")
            (New-CardVisual "cardDlpTotal" 560 20 120 80 500 "DlpPolicies" "Total DLP Policies" "DLP Policies")
            (New-CardVisual "cardDlpBlocked" 695 20 120 80 600 "DlpConnectorRules" "Blocked Connectors" "Blocked")
            # Chart row 1: connector tier donut + top connectors by flow usage
            (New-DonutVisual "donutConnTier" 20 120 430 240 1000 "Connectors" "Tier" "Total Connectors" "Connector Tiers")
            (New-BarChartVisual "barTopConnFlow" 465 120 430 240 2000 "FlowConnectionRefs" "ConnectorId" "Flow Connection Count" "Top Connectors by Flow Usage")
            # Chart row 2: DLP classification donut + DLP policies by rule count
            (New-DonutVisual "donutDlpClass" 910 120 350 240 2500 "DlpConnectorRules" "Classification" "Total Connector Rules" "DLP Classifications")
            # Detail table: DLP connector rules
            (New-TableVisual "tblDlpRules" 20 380 1220 320 4000 "DlpConnectorRules" @("PolicyName","ConnectorName","Classification") "DLP Connector Rules")
        )
    }
    endpoints = @{
        displayName = "Endpoint & API Security"
        visuals = @(
            # Slicer + cards
            (New-SlicerVisual "slicerEnvEndpt" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardEndptActions" 210 20 120 80 100 "FlowActions" "Total Flow Actions" "Flow Actions")
            (New-CardVisual "cardEndptWithUrl" 345 20 120 80 200 "FlowActions" "Actions with Endpoints" "With Endpoints")
            (New-CardVisual "cardEndptHttpRaw" 480 20 120 80 300 "FlowActions" "Flow HTTP Raw Actions" "HTTP Raw")
            (New-CardVisual "cardEndptHttpEntra" 615 20 120 80 400 "FlowActions" "Flow HTTP Entra Actions" "HTTP Entra")
            (New-CardVisual "cardEndptAppRefs" 750 20 120 80 450 "AppConnectorRefs" "Total Connector References" "App Conn Refs")
            (New-CardVisual "cardEndptAppUrl" 885 20 120 80 500 "AppConnectorRefs" "App Refs with Endpoints" "App Endpoints")
            # Donut row: HTTP types for flows + apps
            (New-DonutVisual "donutFlowHttp" 20 120 430 240 1000 "FlowActions" "HttpConnectorType" "Total Flow Actions" "Flow HTTP Connector Types")
            (New-DonutVisual "donutAppHttp" 465 120 430 240 2000 "AppConnectorRefs" "HttpConnectorType" "Total Connector References" "App HTTP Connector Types")
            # Bar: top endpoint domains
            (New-BarChartVisual "barTopDomains" 910 120 350 240 2500 "FlowActions" "BaseUrl" "Total Flow Actions" "Top Endpoint Domains")
            # Detail tables
            (New-TableVisual "tblFlowEndpoints" 20 380 600 320 3000 "FlowActions" @("Name","ActionType","ConnectorId","HttpConnectorType","EndpointUrl","BaseUrl") "Flow Actions with Endpoints")
            (New-TableVisual "tblAppEndpoints" 635 380 625 320 4000 "AppConnectorRefs" @("DisplayName","ConnectorId","HttpConnectorType","EndpointUrl","DataSources") "App Connector Refs with Endpoints")
        )
    }
    risk = @{
        displayName = "Risk & Shadow IT"
        visuals = @(
            # Cards row
            (New-CardVisual "cardRiskOrphApps" 20 20 120 80 100 "Apps" "Orphaned Apps" "Orphaned Apps")
            (New-CardVisual "cardRiskOrphFlows" 155 20 120 80 200 "Flows" "Orphaned Flows" "Orphaned Flows")
            (New-CardVisual "cardRiskStaleApps" 290 20 120 80 300 "Apps" "Stale Apps (90d)" "Stale Apps")
            (New-CardVisual "cardRiskStaleFlows" 425 20 120 80 400 "Flows" "Stale Flows (90d)" "Stale Flows")
            (New-CardVisual "cardRiskBypass" 560 20 120 80 500 "Apps" "Bypass Consent Apps" "Bypass Consent")
            (New-CardVisual "cardRiskUnmanaged" 695 20 120 80 600 "Flows" "Unmanaged Flows" "Unmanaged Flows")
            # Gauge: Shadow IT Rate
            (New-GaugeVisual "gaugeShadowIT" 20 120 280 240 1000 "Environments" "Shadow IT Rate" "Shadow IT Rate")
            # Donuts: managed vs unmanaged, solution-aware
            (New-DonutVisual "donutManagedFlow" 315 120 280 240 2000 "Flows" "ManagedStatus" "Total Flows" "Managed vs Unmanaged Flows")
            (New-DonutVisual "donutSolutionApps" 610 120 280 240 3000 "Apps" "IsSolutionAware" "Total Apps" "Solution Awareness (Apps)")
            # Bar: top owners with unmanaged
            (New-BarChartVisual "barUnmanagedOwners" 905 120 355 240 3500 "Flows" "CreatorDisplayName" "Unmanaged Flows" "Top Owners: Unmanaged Resources")
            # Matrix: Environment risk view
            ,(New-MatrixVisual "matrixRisk" 20 380 1220 160 5000 `
                @(@{Table="Environments"; Column="DisplayName"}) `
                @(@{Table="Apps"; Measure="Orphaned Apps"},
                  @{Table="Flows"; Measure="Stale Flows (90d)"},
                  @{Table="Flows"; Measure="Unmanaged Flows"},
                  @{Table="Environments"; Measure="Unsecured Environments"}) `
                "Risk by Environment")
            # Detail table
            (New-TableVisual "tblRiskItems" 20 555 1220 155 6000 "Apps" @("DisplayName","AppType","OwnerDisplayName","OwnerEmail","StalenessStatus","IsSolutionAware","BypassConsent","EnvironmentName") "Risk Items (Apps)")
        )
    }
    makers = @{
        displayName = "Maker Activity & Ownership"
        visuals = @(
            # Cards row
            (New-CardVisual "cardMakerAppOwners" 20 20 145 80 100 "Apps" "Unique App Owners" "App Owners")
            (New-CardVisual "cardMakerFlowCreators" 180 20 145 80 200 "Flows" "Unique Flow Creators" "Flow Creators")
            (New-CardVisual "cardMakerAgentCreators" 340 20 145 80 300 "CopilotAgents" "Unique Agent Creators" "Agent Creators")
            (New-CardVisual "cardMakerAvgApps" 500 20 145 80 400 "Apps" "Avg Apps Per Owner" "Avg Apps/Owner")
            (New-CardVisual "cardMakerAvgFlows" 660 20 145 80 500 "Flows" "Avg Flows Per Creator" "Avg Flows/Creator")
            # Bar: top makers by total resources (apps)
            (New-BarChartVisual "barTopMakers" 20 120 600 240 1000 "Apps" "OwnerDisplayName" "Total Apps" "Top Makers by App Count")
            # Donut: maker concentration
            (New-DonutVisual "donutMakerConcentration" 635 120 625 240 2000 "Apps" "OwnerDisplayName" "Total Apps" "Maker Concentration")
            # Matrix: top makers breakdown
            ,(New-MatrixVisual "matrixMakers" 20 380 1220 160 3000 `
                @(@{Table="Apps"; Column="OwnerDisplayName"}) `
                @(@{Table="Apps"; Measure="Total Apps"},
                  @{Table="Apps"; Measure="Premium API Apps"},
                  @{Table="Apps"; Measure="Stale Apps (90d)"}) `
                "Maker Activity Matrix")
            # Detail table
            (New-TableVisual "tblMakers" 20 555 1220 155 4000 "Apps" @("OwnerDisplayName","OwnerEmail","EnvironmentName","AppType","SharedUsersCount","IsSolutionAware","LastModifiedTime") "Maker Directory")
        )
    }
    connections = @{
        displayName = "Connection Intelligence"
        visuals = @(
            # Cards row
            (New-CardVisual "cardConnxTotal" 20 20 145 80 100 "Connections" "Total Connections" "Connections")
            (New-CardVisual "cardConnxShared" 180 20 145 80 200 "Connections" "Shared Connections" "Shared")
            (New-CardVisual "cardConnxActive" 340 20 145 80 300 "Connections" "Active Connections" "Active")
            (New-CardVisual "cardConnxTypes" 500 20 145 80 400 "Connections" "Connector Types Used" "Connector Types")
            # Chart row: donut + bar
            (New-DonutVisual "donutConnxStatus" 20 120 430 240 1000 "Connections" "Status" "Total Connections" "Connection Status")
            (New-BarChartVisual "barConnxByConnector" 465 120 430 240 2000 "Connections" "DisplayName" "Total Connections" "Top Connectors by Connection Count")
            # Bar: top connection creators
            (New-BarChartVisual "barConnxCreators" 910 120 350 240 3000 "Connections" "CreatedByName" "Total Connections" "Top Connection Creators")
            # Detail table
            (New-TableVisual "tblConnxDetails" 20 380 1220 320 4000 "Connections" @("DisplayName","CreatedByName","CreatedByEmail","EnvironmentName","Status","IsShared","CreatedTime") "Connection Details")
        )
    }
    # --- Tier 4: Detail/Drill-Down Pages ---
    "env-details" = @{
        displayName = "Environment Detail"
        visuals = @(
            (New-SlicerVisual "slicerEnvDetail" 20 20 170 80 50 "Environments" "DisplayName" "Select Environment")
            (New-CardVisual "cardEnvDetApps" 210 20 105 80 100 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardEnvDetFlows" 330 20 105 80 200 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardEnvDetConn" 450 20 105 80 300 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardEnvDetConnx" 570 20 105 80 350 "Connections" "Total Connections" "Connections")
            (New-CardVisual "cardEnvDetAgents" 690 20 105 80 400 "CopilotAgents" "Total Agents" "Agents")
            (New-CardVisual "cardEnvDetCap" 810 20 90 80 500 "Environments" "Total Capacity GB" "Capacity GB")
            # Charts
            (New-BarChartVisual "barEnvDetAppType" 20 120 430 240 1000 "Apps" "AppType" "Total Apps" "Apps by Type")
            (New-BarChartVisual "barEnvDetFlowState" 465 120 430 240 2000 "Flows" "State" "Total Flows" "Flows by State")
            # Tables
            (New-TableVisual "tblEnvDetApps" 20 380 400 320 3000 "Apps" @("DisplayName","AppType","OwnerDisplayName","Status") "Apps in Environment")
            (New-TableVisual "tblEnvDetFlows" 435 380 400 320 4000 "Flows" @("DisplayName","State","TriggerType","CreatorDisplayName") "Flows in Environment")
            (New-TableVisual "tblEnvDetAgents" 850 380 410 320 5000 "CopilotAgents" @("DisplayName","AgentType","State","TotalComponents") "Agents in Environment")
        )
    }
    "app-details" = @{
        displayName = "App Detail"
        visuals = @(
            (New-SlicerVisual "slicerAppDetail" 20 20 170 80 50 "Apps" "DisplayName" "Select App")
            (New-CardVisual "cardAppDetShared" 210 20 120 80 100 "Apps" "Total Shared Users" "Shared Users")
            (New-CardVisual "cardAppDetGroups" 345 20 120 80 200 "Apps" "Avg Shared Users" "Shared Groups")
            (New-CardVisual "cardAppDetConnRefs" 480 20 120 80 300 "AppConnectorRefs" "Total Connector References" "Connector Refs")
            # App info table
            (New-TableVisual "tblAppInfo" 20 120 1220 260 1000 "Apps" @("DisplayName","AppType","Status","OwnerDisplayName","OwnerEmail","EnvironmentName","SharedUsersCount","SharedGroupsCount","UsesPremiumApi","UsesCustomApi","IsSolutionAware","BypassConsent","AppVersion","CreatedTime","LastModifiedTime") "App Info")
            # App connector references
            (New-TableVisual "tblAppConnRefs" 20 400 1220 300 2000 "AppConnectorRefs" @("DisplayName","ConnectorId","HttpConnectorType","EndpointUrl","DataSources") "App Connector References")
        )
    }
    "flow-details" = @{
        displayName = "Flow Detail"
        visuals = @(
            (New-SlicerVisual "slicerFlowDetail" 20 20 170 80 50 "Flows" "DisplayName" "Select Flow")
            (New-CardVisual "cardFlowDetActions" 210 20 120 80 100 "FlowActions" "Total Flow Actions" "Actions")
            (New-CardVisual "cardFlowDetTriggers" 345 20 120 80 200 "FlowTriggers" "Total Flow Triggers" "Triggers")
            (New-CardVisual "cardFlowDetConnRefs" 480 20 120 80 300 "FlowConnectionRefs" "Total Flow Connections" "Connection Refs")
            # Flow info table
            (New-TableVisual "tblFlowInfo" 20 120 1220 200 1000 "Flows" @("DisplayName","State","CreatorDisplayName","TriggerType","EnvironmentName","IsSolutionAware","IsManaged","SuspensionReason","CreatedTime","LastModifiedTime") "Flow Info")
            # Flow actions + triggers
            (New-TableVisual "tblFlowActions" 20 340 610 360 2000 "FlowActions" @("Name","ActionType","ConnectorId","BaseUrl","EndpointUrl","OperationId") "Flow Actions")
            (New-TableVisual "tblFlowTriggers" 645 340 595 360 3000 "FlowTriggers" @("Name","TriggerType","ConnectorId","BaseUrl","EndpointUrl","OperationId") "Flow Triggers")
        )
    }
    "agent-details" = @{
        displayName = "Agent Detail"
        visuals = @(
            (New-SlicerVisual "slicerAgentDetail" 20 20 170 80 50 "CopilotAgents" "DisplayName" "Select Agent")
            (New-CardVisual "cardAgentDetTopics" 210 20 120 80 100 "CopilotAgents" "Total Topics" "Topics")
            (New-CardVisual "cardAgentDetKnow" 345 20 120 80 200 "CopilotAgents" "Total Knowledge Sources" "Knowledge")
            (New-CardVisual "cardAgentDetSkills" 480 20 120 80 300 "CopilotAgents" "Total Skills" "Skills")
            (New-CardVisual "cardAgentDetGPT" 615 20 120 80 400 "CopilotComponents" "Custom GPT Components" "Custom GPT")
            (New-CardVisual "cardAgentDetComps" 750 20 120 80 500 "CopilotAgents" "Total Agent Components" "Components")
            # Agent info table
            (New-TableVisual "tblAgentInfo" 20 120 1220 240 1000 "CopilotAgents" @("DisplayName","AgentType","SchemaName","State","StatusReason","Language","AuthenticationMode","AuthenticationTrigger","AccessControlPolicy","RuntimeProvider","Origin","Template","IsManaged","SolutionId","PublishedOn","PublishedByName","CreatedOn","CreatedByName","ModifiedOn") "Agent Info")
            # Agent components
            (New-TableVisual "tblAgentComps" 20 380 1220 320 2000 "CopilotComponents" @("Name","ComponentType","Category","Description","Status","IsManaged","CreatedOn","ModifiedOn") "Agent Components")
        )
    }
    "dlp-details" = @{
        displayName = "DLP Policy Detail"
        visuals = @(
            (New-SlicerVisual "slicerDlpDetail" 20 20 170 80 50 "DlpPolicies" "DisplayName" "Select Policy")
            (New-CardVisual "cardDlpDetRules" 210 20 120 80 100 "DlpConnectorRules" "Total Connector Rules" "Rules")
            (New-CardVisual "cardDlpDetBiz" 345 20 120 80 200 "DlpConnectorRules" "Business Connectors" "Business")
            (New-CardVisual "cardDlpDetNonBiz" 480 20 120 80 300 "DlpConnectorRules" "Non-Business Connectors" "Non-Business")
            (New-CardVisual "cardDlpDetBlocked" 615 20 120 80 400 "DlpConnectorRules" "Blocked Connectors" "Blocked")
            # Policy info table
            (New-TableVisual "tblDlpInfo" 20 120 1220 200 1000 "DlpPolicies" @("DisplayName","Description","IsEnabled","PolicyType","EnvironmentScope","CreatedTime","LastModifiedTime") "Policy Info")
            # Connector rules
            (New-TableVisual "tblDlpConnRules" 20 340 1220 360 2000 "DlpConnectorRules" @("ConnectorName","Classification","PolicyName","ConnectorId") "Connector Rules for Policy")
        )
    }
    "connector-details" = @{
        displayName = "Connector Detail"
        visuals = @(
            (New-SlicerVisual "slicerConnDetail" 20 20 170 80 50 "Connectors" "DisplayName" "Select Connector")
            (New-CardVisual "cardConnDetConnx" 210 20 120 80 100 "Connections" "Total Connections" "Connections")
            (New-CardVisual "cardConnDetFlowRefs" 345 20 120 80 200 "FlowConnectionRefs" "Total Flow Connections" "Flow Refs")
            (New-CardVisual "cardConnDetAppRefs" 480 20 120 80 300 "AppConnectorRefs" "Total Connector References" "App Refs")
            # Connections using this connector
            (New-TableVisual "tblConnDetConnx" 20 120 1220 280 1000 "Connections" @("DisplayName","CreatedByName","CreatedByEmail","EnvironmentName","Status","IsShared","CreatedTime") "Connections Using This Connector")
            # Flows using this connector
            (New-TableVisual "tblConnDetFlows" 20 420 1220 280 2000 "FlowConnectionRefs" @("FlowKey","ConnectorId","ConnectionName","ConnectionUrl","EnvironmentId") "Flows Using This Connector")
        )
    }
}

# --- Generate page files ---

foreach ($pageName in $pageNames) {
    $pageDef = $pageDefs[$pageName]
    $pageDir = "$defDir/pages/$pageName"

    Write-JsonFile "$pageDir/page.json" ([ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.3.0/schema.json"
        name = $pageName
        displayName = $pageDef.displayName
        displayOption = "FitToPage"
        height = 720
        width = 1280
    })

    foreach ($visual in $pageDef.visuals) {
        $visualName = $visual.name
        Write-JsonFile "$pageDir/visuals/$visualName/visual.json" $visual
    }
}

# ============================================================================
# DONE
# ============================================================================

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Enterprise Governance PBIP v3 created!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Project: $OutputPath/$projectName.pbip" -ForegroundColor Cyan
Write-Host "CSV source: $CsvPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "To open:" -ForegroundColor Yellow
Write-Host "  1. Enable Developer Mode in Power BI Desktop:" -ForegroundColor Gray
Write-Host "     File > Options > Preview features > Power BI Project (.pbip)" -ForegroundColor Gray
Write-Host "  2. Open: $OutputPath/$projectName.pbip" -ForegroundColor Gray
Write-Host "  3. If CSVs move, update the CsvFolderPath parameter in Transform Data" -ForegroundColor Gray
Write-Host ""
Write-Host "16 Pages (1 command center + 6 domain + 3 intelligence + 6 detail):" -ForegroundColor Yellow
Write-Host "  Tier 1 - Command Center:" -ForegroundColor Cyan
Write-Host "   1. Executive Command Center — governance score, flow health, security coverage" -ForegroundColor Gray
Write-Host "  Tier 2 - Domain Governance:" -ForegroundColor Cyan
Write-Host "   2. Environment Health       — types, capacity, security rate, unsecured envs" -ForegroundColor Gray
Write-Host "   3. App Portfolio            — types, owners, staleness, solution awareness" -ForegroundColor Gray
Write-Host "   4. Flow Operations          — states, suspension, triggers, activity trends" -ForegroundColor Gray
Write-Host "   5. Copilot Agents           — types, auth modes, components, creators" -ForegroundColor Gray
Write-Host "   6. Connector & DLP Gov.     — tiers, flow usage, DLP classifications" -ForegroundColor Gray
Write-Host "   7. Endpoint & API Security  — HTTP types, endpoint domains, action details" -ForegroundColor Gray
Write-Host "  Tier 3 - Risk & Intelligence:" -ForegroundColor Cyan
Write-Host "   8. Risk & Shadow IT         — orphans, staleness, shadow IT rate, risk matrix" -ForegroundColor Gray
Write-Host "   9. Maker Activity           — concentration, ownership, maker directory" -ForegroundColor Gray
Write-Host "  10. Connection Intelligence  — status, shared connections, top connectors" -ForegroundColor Gray
Write-Host "  Tier 4 - Detail/Drill-Down:" -ForegroundColor Cyan
Write-Host "  11. Environment Detail       — select env, view apps/flows/agents" -ForegroundColor Gray
Write-Host "  12. App Detail               — select app, view properties + connectors" -ForegroundColor Gray
Write-Host "  13. Flow Detail              — select flow, view actions + triggers" -ForegroundColor Gray
Write-Host "  14. Agent Detail             — select agent, view info + components" -ForegroundColor Gray
Write-Host "  15. DLP Policy Detail        — select policy, view rules" -ForegroundColor Gray
Write-Host "  16. Connector Detail         — select connector, view connections + flow refs" -ForegroundColor Gray
Write-Host ""
Write-Host "Key Governance Measures:" -ForegroundColor Yellow
Write-Host "  - Governance Score (weighted: security + DLP + flow health + solutions + lifecycle)" -ForegroundColor Gray
Write-Host "  - Shadow IT Rate, Flow Health Rate, Security Coverage" -ForegroundColor Gray
Write-Host "  - Orphaned Apps/Flows, Stale Assets, Bypass Consent, Unmanaged Flows" -ForegroundColor Gray
Write-Host "  - DLP Coverage Rate, Connector Utilization, Overall Solution Coverage" -ForegroundColor Gray
Write-Host ""
Write-Host "Tables (14): Environments, Apps, Flows, Connectors, DlpPolicies," -ForegroundColor Yellow
Write-Host "  DlpConnectorRules, UsageAnalytics, AppConnectorRefs, FlowActions," -ForegroundColor Gray
Write-Host "  FlowTriggers, FlowConnectionRefs, CopilotAgents, CopilotComponents," -ForegroundColor Gray
Write-Host "  Connections (NEW)" -ForegroundColor Green
Write-Host ""
Write-Host "Relationships (12 auto-configured):" -ForegroundColor Yellow
Write-Host "  Apps -> Environments | Flows -> Environments | Connectors -> Environments" -ForegroundColor Gray
Write-Host "  DlpConnectorRules -> DlpPolicies | UsageAnalytics -> Environments" -ForegroundColor Gray
Write-Host "  AppConnectorRefs -> Apps | FlowActions -> Flows | FlowTriggers -> Flows" -ForegroundColor Gray
Write-Host "  FlowConnectionRefs -> Flows | CopilotAgents -> Environments" -ForegroundColor Gray
Write-Host "  CopilotComponents -> CopilotAgents | Connections -> Environments (NEW)" -ForegroundColor Green
Write-Host ""
