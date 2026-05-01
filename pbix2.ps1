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

function New-CsvPartitionOptional {
    param([string]$TableName, [hashtable[]]$TypeMappings)
    $typeLines = ($TypeMappings | ForEach-Object {
        "        {`"$($_.Name)`", $($_.Type)}"
    }) -join ",`n"

    $emptyCols = ($TypeMappings | ForEach-Object { "`"$($_.Name)`"" }) -join ", "

    $mExpr = [System.Collections.Generic.List[string]]::new()
    $mExpr.Add("let")
    $mExpr.Add("    Source = try Csv.Document(File.Contents(CsvFolderPath & `"$TableName.csv`"), [Delimiter=`",`", Encoding=65001, QuoteStyle=QuoteStyle.Csv]) otherwise null,")
    $mExpr.Add("    EmptyTable = #table({$emptyCols}, {}),")
    $mExpr.Add("    Headers = if Source = null then EmptyTable else Table.PromoteHeaders(Source, [PromoteAllScalars=true]),")
    $mExpr.Add("    Typed = Table.TransformColumnTypes(Headers, {")
    $mExpr.Add($typeLines)
    $mExpr.Add("    }, `"en-US`")")
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
          [hashtable[]]$RowFields, [hashtable[]]$ValueFields, [string]$Title = $null,
          [hashtable[]]$ColumnFields = $null)
    $rowProjections = $RowFields | ForEach-Object { New-Projection $_.Table $_.Column "Column" }
    $valProjections = $ValueFields | ForEach-Object { New-Projection $_.Table $_.Measure "Measure" }
    $queryState = [ordered]@{
        Rows = @{ projections = @($rowProjections) }
        Values = @{ projections = @($valProjections) }
    }
    if ($ColumnFields) {
        $colProjections = $ColumnFields | ForEach-Object { New-Projection $_.Table $_.Column "Column" }
        $queryState.Columns = @{ projections = @($colProjections) }
    }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "pivotTable"
            query = [ordered]@{ queryState = $queryState }
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
# ADVANCED VISUAL HELPERS — KPI, Funnel, Stacked, Ribbon, Waterfall, Scatter,
# TextBox, Image, Shape, Button, Decomposition Tree, Smart Narrative, Q&A
# ============================================================================

function New-KpiVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$IndicatorMeasure, [string]$TrendCategoryTable,
          [string]$TrendCategoryCol, [string]$TargetMeasure = $null, [string]$Title = $null)
    $queryState = [ordered]@{
        Indicator = @{ projections = @(,(New-Projection $Table $IndicatorMeasure "Measure")) }
        TrendLine = @{ projections = @(,(New-Projection $TrendCategoryTable $TrendCategoryCol "Column")) }
    }
    if ($TargetMeasure) {
        $queryState.Goal = @{ projections = @(,(New-Projection $Table $TargetMeasure "Measure")) }
    }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "kpi"
            query = [ordered]@{ queryState = $queryState }
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

function New-FunnelVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "funnel"
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

function New-StackedColumnChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$LegendTable, [string]$LegendCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "stackedColumnChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $CatTable $CategoryCol "Column")) }
                    Series = @{ projections = @(,(New-Projection $LegendTable $LegendCol "Column")) }
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

function New-StackedBarChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$LegendTable, [string]$LegendCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "barChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $CatTable $CategoryCol "Column")) }
                    Series = @{ projections = @(,(New-Projection $LegendTable $LegendCol "Column")) }
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

function New-RibbonChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$LegendTable, [string]$LegendCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "ribbonChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $CatTable $CategoryCol "Column")) }
                    Series = @{ projections = @(,(New-Projection $LegendTable $LegendCol "Column")) }
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

function New-WaterfallVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "waterfallChart"
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

function New-AreaChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$CatTable, [string]$CategoryCol,
          [string]$ValTable, [string]$ValueMeasure, [string]$Title = $null)
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "areaChart"
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

function New-ScatterVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$DetailsTable, [string]$DetailsCol,
          [string]$XTable, [string]$XMeasure,
          [string]$YTable, [string]$YMeasure,
          [string]$SizeTable = $null, [string]$SizeMeasure = $null, [string]$Title = $null)
    $queryState = [ordered]@{
        Category = @{ projections = @(,(New-Projection $DetailsTable $DetailsCol "Column")) }
        X = @{ projections = @(,(New-Projection $XTable $XMeasure "Measure")) }
        Y = @{ projections = @(,(New-Projection $YTable $YMeasure "Measure")) }
    }
    if ($SizeMeasure) {
        $queryState.Size = @{ projections = @(,(New-Projection $SizeTable $SizeMeasure "Measure")) }
    }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "scatterChart"
            query = [ordered]@{ queryState = $queryState }
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

# Text box (rich text) — useful for headings, callouts, glossary entries
function New-TextBoxVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z, [string]$Text,
          [string]$FontSize = "14", [string]$Color = "#252423", [bool]$Bold = $false)
    $weight = if ($Bold) { "bold" } else { "normal" }
    $dq = '"'
    $html = "'<p style=" + $dq + "font-size:" + $FontSize + "px;color:" + $Color + ";font-weight:" + $weight + $dq + ">" + $Text + "</p>'"
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "textbox"
            objects = @{
                general = @(@{ properties = @{
                    paragraphs = @{
                        expr = @{ Literal = @{ Value = $html } }
                    }
                } })
            }
            drillFilterOtherVisuals = $true
        }
    }
}

# Multi-row card (good for glossary / KPI definitions)
function New-MultiRowCardVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string[]]$Columns, [string]$Title = $null)
    $projections = $Columns | ForEach-Object { New-Projection $Table $_ "Column" }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "multiRowCard"
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

# Card visual variant that supports multiple measures side-by-side
function New-MultiKpiCardVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [hashtable[]]$Measures, [string]$Title = $null)
    $projections = $Measures | ForEach-Object { New-Projection $_.Table $_.Measure "Measure" }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "multiRowCard"
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

# Decomposition tree — drill-down by hierarchy
function New-DecompositionTreeVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$AnalyzeTable, [string]$AnalyzeMeasure,
          [hashtable[]]$ExplainBy, [string]$Title = $null)
    $explainProjections = $ExplainBy | ForEach-Object { New-Projection $_.Table $_.Column "Column" }
    $vis = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "decompositionTreeVisual"
            query = [ordered]@{
                queryState = [ordered]@{
                    Analyze = @{ projections = @(,(New-Projection $AnalyzeTable $AnalyzeMeasure "Measure")) }
                    Explain = @{ projections = @($explainProjections) }
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

# Conditional-formatting-enabled table (data bars on the first numeric col, color scale on second)
function New-FormattedTableVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string[]]$Columns, [string[]]$DataBarColumns = @(),
          [string]$Title = $null)
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
    if ($DataBarColumns.Count -gt 0) {
        $valuesObjs = @()
        foreach ($col in $DataBarColumns) {
            # Build the deeply-nested data-bar fillRule in stages for readability
            $colorExpr = @{ Literal = @{ Value = "'#118DFF'" } }
            $minColor = @{ color = @{ expr = $colorExpr } }
            $maxColor = @{ color = @{ expr = $colorExpr } }
            $linearGrad = @{ linearGradient2 = @{ min = $minColor; max = $maxColor } }
            $inputCol = @{ Column = @{ Expression = (New-SourceRef $Table); Property = $col } }
            $fillRule = @{ FillRule = @{ Input = $inputCol; FillRule = $linearGrad } }
            $valuesObjs += @{
                selector = @{ metadata = "$Table.$col" }
                properties = @{ dataBars = @{ expr = $fillRule } }
            }
        }
        $vis.visual.objects = @{ values = $valuesObjs }
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

# Action button (page navigation)
function New-NavButtonVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Text, [string]$TargetPageName)
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "actionButton"
            objects = @{
                text = @(@{ properties = @{
                    show = @{ expr = @{ Literal = @{ Value = "true" } } }
                    text = @{ expr = @{ Literal = @{ Value = "'$Text'" } } }
                } })
            }
            visualContainerObjects = @{
                visualLink = @(@{ properties = @{
                    show = @{ expr = @{ Literal = @{ Value = "true" } } }
                    type = @{ expr = @{ Literal = @{ Value = "'PageNavigation'" } } }
                    navigationSection = @{ expr = @{ Literal = @{ Value = "'$TargetPageName'" } } }
                } })
            }
        }
    }
}

# Page-level filter — used for drillthrough configuration
function New-PageFilter {
    param([string]$Name, [string]$Table, [string]$Column, [string]$FilterType = "Categorical")
    [ordered]@{
        name = $Name
        field = (New-ColField $Table $Column)
        type = $FilterType
        howCreated = "Drilled"
    }
}

# Top-N filter
function New-TopNFilter {
    param([string]$Name, [string]$Table, [string]$Column, [int]$N,
          [string]$ByTable, [string]$ByMeasure)
    [ordered]@{
        name = $Name
        field = (New-ColField $Table $Column)
        type = "TopN"
        filter = @{
            Version = 2
            From = @(@{ Name = "t"; Entity = $Table; Type = 0 })
            Where = @(@{
                Condition = @{
                    TopN = @{
                        ItemCount = $N
                        Expression = @{
                            Column = @{
                                Expression = @{ SourceRef = @{ Source = "t" } }
                                Property = $Column
                            }
                        }
                    }
                }
                Target = @(@{
                    Measure = @{
                        Expression = @{ SourceRef = @{ Entity = $ByTable } }
                        Property = $ByMeasure
                    }
                })
            })
        }
        filterType = 5
    }
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
        # Calculated columns for slicing
        (New-CalcColumnDef "TotalCapacityMb" "'Environments'[DatabaseUsedMb] + 'Environments'[FileUsedMb] + 'Environments'[LogUsedMb]" "double")
        (New-CalcColumnDef "CapacityBand" "SWITCH(TRUE(), 'Environments'[TotalCapacityMb] = 0, `"Empty`", 'Environments'[TotalCapacityMb] < 1024, `"Low (<1GB)`", 'Environments'[TotalCapacityMb] < 10240, `"Medium (1-10GB)`", 'Environments'[TotalCapacityMb] < 51200, `"High (10-50GB)`", `"Very High (>50GB)`")")
        (New-CalcColumnDef "ResourceTotal" "COALESCE(CALCULATE(COUNTROWS('Apps')), 0) + COALESCE(CALCULATE(COUNTROWS('Flows')), 0) + COALESCE(CALCULATE(COUNTROWS('CopilotAgents')), 0)" "int64")
        (New-CalcColumnDef "IsEmpty" "'Environments'[ResourceTotal] = 0" "boolean")
        (New-CalcColumnDef "IsSecured" "NOT(ISBLANK('Environments'[SecurityGroupId]) || 'Environments'[SecurityGroupId] = `"`")" "boolean")
        (New-CalcColumnDef "EnvDensity" "DIVIDE('Environments'[ResourceTotal], DIVIDE('Environments'[TotalCapacityMb], 1024), 0)" "double")
        (New-CalcColumnDef "EnvAgeDays" "IF(ISBLANK('Environments'[CreatedTime]), BLANK(), DATEDIFF('Environments'[CreatedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "EnvLifecycleStage" "SWITCH(TRUE(), 'Environments'[EnvAgeDays] < 30, `"New`", 'Environments'[EnvAgeDays] < 365, `"Active`", 'Environments'[EnvAgeDays] < 1095, `"Mature`", `"Legacy`")")
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
        (New-MeasureDef "Total Log MB" "SUM('Environments'[LogUsedMb])" "#,##0.0" "Capacity")
        (New-MeasureDef "Total Capacity GB" "DIVIDE(SUM('Environments'[DatabaseUsedMb]) + SUM('Environments'[FileUsedMb]) + SUM('Environments'[LogUsedMb]), 1024, 0)" "#,##0.00" "Capacity")
        # New governance measures
        (New-MeasureDef "Unsecured Environments" "CALCULATE(COUNTROWS('Environments'), ISBLANK('Environments'[SecurityGroupId]) || 'Environments'[SecurityGroupId] = `"`") + 0")
        (New-MeasureDef "Env Security Rate" "DIVIDE([Total Environments] - [Unsecured Environments], [Total Environments], 0)" "0.0%")
        (New-MeasureDef "Default Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[IsDefault] = TRUE())")
        (New-MeasureDef "Developer Environments" "CALCULATE(COUNTROWS('Environments'), 'Environments'[EnvironmentType] = `"Developer`")")
        # Cross-table composite measures (hub table)
        (New-MeasureDef "Governance Score" "VAR SecurityScore = [Env Security Rate] * 20 VAR DlpScore = [DLP Coverage Rate] * 20 VAR FlowHealthScore = (1 - [Suspension Rate]) * 20 VAR SolutionScore = DIVIDE([Solution-Aware Flows] + [Solution-Aware Apps], [Total Flows] + [Total Apps], 0) * 20 VAR LifecycleScore = (1 - DIVIDE([Stale Apps (90d)] + [Stale Flows (90d)], [Total Apps] + [Total Flows], 0)) * 20 RETURN ROUND(SecurityScore + DlpScore + FlowHealthScore + SolutionScore + LifecycleScore, 0)" "0" "Governance")
        (New-MeasureDef "Governance Score v2" "VAR Sec = [Env Security Rate] * 15 VAR Dlp = [DLP Coverage Rate] * 15 VAR Flw = (1 - [Suspension Rate]) * 10 VAR Sol = [Overall Solution Coverage] * 15 VAR Life = (1 - DIVIDE([Stale Apps (90d)] + [Stale Flows (90d)], [Total Apps] + [Total Flows], 0)) * 10 VAR Sha = (1 - DIVIDE([Widely Shared Apps], [Total Apps], 0)) * 10 VAR Http = (1 - DIVIDE([Flow HTTP Raw Actions] + [Flow HTTP Webhook Actions], [Total Flow Actions], 0)) * 10 VAR Orph = (1 - DIVIDE([Orphaned Apps] + [Orphaned Flows], [Total Apps] + [Total Flows], 0)) * 5 VAR Prem = (1 - DIVIDE([Premium API Apps], [Total Apps], 0)) * 5 VAR Auth = DIVIDE([Total Agents] - [Auth Risk Agents], [Total Agents], 0) * 5 RETURN ROUND(Sec + Dlp + Flw + Sol + Life + Sha + Http + Orph + Prem + Auth, 0)" "0" "Governance")
        (New-MeasureDef "Flow Health Rate" "1 - [Suspension Rate]" "0.0%" "Governance")
        (New-MeasureDef "Shadow IT Rate" "DIVIDE([Unmanaged Flows] + CALCULATE(COUNTROWS('Apps'), 'Apps'[IsSolutionAware] = FALSE()), [Total Flows] + [Total Apps], 0)" "0.0%" "Risk")
        (New-MeasureDef "Total Resources" "[Total Apps] + [Total Flows] + [Total Agents]" "#,##0" "Metrics")
        (New-MeasureDef "Overall Solution Coverage" "DIVIDE([Solution-Aware Apps] + [Solution-Aware Flows], [Total Apps] + [Total Flows], 0)" "0.0%" "Governance")
        (New-MeasureDef "Security Coverage" "[Env Security Rate]" "0.0%" "Governance")
        # New: capacity & sprawl
        (New-MeasureDef "Avg Capacity GB Per Env" "DIVIDE([Total Capacity GB], [Total Environments], 0)" "#,##0.00" "Capacity")
        (New-MeasureDef "Empty Environments" "CALCULATE(COUNTROWS('Environments'), FILTER('Environments', 'Environments'[ResourceTotal] = 0)) + 0" "#,##0" "Sprawl")
        (New-MeasureDef "Empty Env Rate" "DIVIDE([Empty Environments], [Total Environments], 0)" "0.0%" "Sprawl")
        (New-MeasureDef "Region Count" "DISTINCTCOUNT('Environments'[Region])" "#,##0" "Sprawl")
        (New-MeasureDef "Sprawl Index" "DIVIDE([Total Environments], [Region Count], 0)" "0.0" "Sprawl")
        (New-MeasureDef "Heavy Capacity Envs (>50GB)" "CALCULATE(COUNTROWS('Environments'), 'Environments'[DatabaseUsedMb] + 'Environments'[FileUsedMb] + 'Environments'[LogUsedMb] > 50000) + 0" "#,##0" "Capacity")
        (New-MeasureDef "Top Capacity Env" "CALCULATE(MAX('Environments'[DatabaseUsedMb]) + MAX('Environments'[FileUsedMb]) + MAX('Environments'[LogUsedMb])) / 1024" "#,##0.0" "Capacity")
        (New-MeasureDef "DB to File Ratio" "DIVIDE([Total Database MB], [Total File MB], 0)" "0.00" "Capacity")
        # New: collection telemetry
        (New-MeasureDef "Last Collected" "FORMAT(MAX('Environments'[CollectedAt]), `"yyyy-MM-dd HH:mm`")" $null "Telemetry")
        (New-MeasureDef "Days Since Collection" "DATEDIFF(MAX('Environments'[CollectedAt]), TODAY(), DAY)" "0" "Telemetry")
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
        # Calculated columns for lifecycle / segmentation
        (New-CalcColumnDef "CreatedDate" "DATE(YEAR('Apps'[CreatedTime]), MONTH('Apps'[CreatedTime]), DAY('Apps'[CreatedTime]))" "dateTime")
        (New-CalcColumnDef "ModifiedDate" "DATE(YEAR('Apps'[LastModifiedTime]), MONTH('Apps'[LastModifiedTime]), DAY('Apps'[LastModifiedTime]))" "dateTime")
        (New-CalcColumnDef "PublishedDate" "IF(ISBLANK('Apps'[LastPublishedTime]), BLANK(), DATE(YEAR('Apps'[LastPublishedTime]), MONTH('Apps'[LastPublishedTime]), DAY('Apps'[LastPublishedTime])))" "dateTime")
        (New-CalcColumnDef "StalenessStatus" "IF('Apps'[LastModifiedTime] < TODAY() - 90, `"Stale`", `"Active`")")
        (New-CalcColumnDef "AppAgeDays" "IF(ISBLANK('Apps'[CreatedTime]), BLANK(), DATEDIFF('Apps'[CreatedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "DaysSinceModified" "IF(ISBLANK('Apps'[LastModifiedTime]), BLANK(), DATEDIFF('Apps'[LastModifiedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "DaysSincePublished" "IF(ISBLANK('Apps'[LastPublishedTime]), BLANK(), DATEDIFF('Apps'[LastPublishedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "LifecyclePhase" "SWITCH(TRUE(), 'Apps'[DaysSinceModified] < 7, `"01-New (<7d)`", 'Apps'[DaysSinceModified] < 30, `"02-Recent (<30d)`", 'Apps'[DaysSinceModified] < 90, `"03-Active (<90d)`", 'Apps'[DaysSinceModified] < 365, `"04-Mature (<1y)`", `"05-Stale (>1y)`")")
        (New-CalcColumnDef "SharingRiskBand" "SWITCH(TRUE(), 'Apps'[SharedUsersCount] = 0 && 'Apps'[SharedGroupsCount] = 0, `"01-Private`", 'Apps'[SharedUsersCount] < 10 && 'Apps'[SharedGroupsCount] < 2, `"02-Team`", 'Apps'[SharedUsersCount] < 100, `"03-Org`", `"04-Tenant-wide`")")
        (New-CalcColumnDef "IsOrphaned" "ISBLANK('Apps'[OwnerEmail]) || 'Apps'[OwnerEmail] = `"`"" "boolean")
        (New-CalcColumnDef "RiskScore" "VAR Stale = IF('Apps'[DaysSinceModified] >= 90, 2, 0) VAR Orph = IF('Apps'[IsOrphaned], 3, 0) VAR Shared = SWITCH(TRUE(), 'Apps'[SharedUsersCount] >= 100, 3, 'Apps'[SharedUsersCount] >= 10, 1, 0) VAR NoSol = IF('Apps'[IsSolutionAware] = FALSE(), 1, 0) VAR Bypass = IF('Apps'[BypassConsent] = TRUE(), 2, 0) RETURN Stale + Orph + Shared + NoSol + Bypass" "int64")
        (New-CalcColumnDef "RiskBand" "SWITCH(TRUE(), 'Apps'[RiskScore] = 0, `"01-None`", 'Apps'[RiskScore] <= 2, `"02-Low`", 'Apps'[RiskScore] <= 4, `"03-Medium`", `"04-High`")")
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
        # Time intelligence (active rel: Calendar -> Apps[CreatedTime])
        (New-MeasureDef "Apps Created MTD" "CALCULATE([Total Apps], DATESMTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created QTD" "CALCULATE([Total Apps], DATESQTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created YTD" "CALCULATE([Total Apps], DATESYTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created Last 7d" "CALCULATE([Total Apps], DATESINPERIOD('Calendar'[Date], TODAY(), -7, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created Last 30d Calendar" "CALCULATE([Total Apps], DATESINPERIOD('Calendar'[Date], TODAY(), -30, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created Last 90d" "CALCULATE([Total Apps], DATESINPERIOD('Calendar'[Date], TODAY(), -90, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps Created PYTD" "CALCULATE([Total Apps], DATESYTD(SAMEPERIODLASTYEAR('Calendar'[Date])))" "#,##0" "Time Intel")
        (New-MeasureDef "Apps YoY Change" "[Apps Created YTD] - [Apps Created PYTD]" "+#,##0;-#,##0;0" "Time Intel")
        (New-MeasureDef "Apps YoY %" "DIVIDE([Apps YoY Change], [Apps Created PYTD])" "+0.0%;-0.0%;0.0%" "Time Intel")
        # Modified-time tracking (uses inactive rel)
        (New-MeasureDef "Apps Modified Last 30d" "CALCULATE([Total Apps], DATESINPERIOD('Calendar'[Date], TODAY(), -30, DAY), USERELATIONSHIP('Calendar'[Date], 'Apps'[ModifiedDate]))" "#,##0" "Time Intel")
        # Sharing & risk
        (New-MeasureDef "Tenant-wide Shared Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[SharingRiskBand] = `"04-Tenant-wide`") + 0" "#,##0" "Sharing")
        (New-MeasureDef "Org-Shared Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[SharingRiskBand] = `"03-Org`") + 0" "#,##0" "Sharing")
        (New-MeasureDef "Private Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[SharingRiskBand] = `"01-Private`") + 0" "#,##0" "Sharing")
        (New-MeasureDef "Avg Sharing Breadth" "AVERAGE('Apps'[SharedUsersCount])" "#,##0.0" "Sharing")
        (New-MeasureDef "Max Sharing Breadth" "MAX('Apps'[SharedUsersCount])" "#,##0" "Sharing")
        (New-MeasureDef "App Risk Score Total" "SUMX('Apps', 'Apps'[RiskScore])" "#,##0" "Risk")
        (New-MeasureDef "Avg App Risk" "AVERAGE('Apps'[RiskScore])" "0.00" "Risk")
        (New-MeasureDef "High Risk Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[RiskBand] = `"04-High`") + 0" "#,##0" "Risk")
        (New-MeasureDef "Medium Risk Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[RiskBand] = `"03-Medium`") + 0" "#,##0" "Risk")
        # Pareto / concentration
        (New-MeasureDef "Maker App Rank" "RANKX(ALL('Apps'[OwnerDisplayName]), [Total Apps],, DESC)" "#,##0" "Makers")
        (New-MeasureDef "Top 5 Makers App Count" "VAR R = [Maker App Rank] RETURN IF(R <= 5, [Total Apps], BLANK())" "#,##0" "Makers")
        (New-MeasureDef "Cumulative Apps by Maker" "CALCULATE([Total Apps], FILTER(ALLSELECTED('Apps'[OwnerDisplayName]), [Maker App Rank] <= MAX('Apps'[OwnerDisplayName].[OwnerDisplayName])))" "#,##0" "Makers")
        # Premium licensing
        (New-MeasureDef "Premium Maker Count" "CALCULATE(DISTINCTCOUNT('Apps'[OwnerObjectId]), 'Apps'[UsesPremiumApi] = TRUE())" "#,##0" "Premium")
        (New-MeasureDef "Premium Exposure Rate" "DIVIDE([Premium API Apps], [Total Apps], 0)" "0.0%" "Premium")
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
        # Calculated columns
        (New-CalcColumnDef "CreatedDate" "DATE(YEAR('Flows'[CreatedTime]), MONTH('Flows'[CreatedTime]), DAY('Flows'[CreatedTime]))" "dateTime")
        (New-CalcColumnDef "ModifiedDate" "DATE(YEAR('Flows'[LastModifiedTime]), MONTH('Flows'[LastModifiedTime]), DAY('Flows'[LastModifiedTime]))" "dateTime")
        (New-CalcColumnDef "ManagedStatus" "IF('Flows'[IsManaged] = TRUE(), `"Managed`", `"Unmanaged`")")
        (New-CalcColumnDef "FlowAgeDays" "IF(ISBLANK('Flows'[CreatedTime]), BLANK(), DATEDIFF('Flows'[CreatedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "DaysSinceModified" "IF(ISBLANK('Flows'[LastModifiedTime]), BLANK(), DATEDIFF('Flows'[LastModifiedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "LifecyclePhase" "SWITCH(TRUE(), 'Flows'[DaysSinceModified] < 7, `"01-New (<7d)`", 'Flows'[DaysSinceModified] < 30, `"02-Recent (<30d)`", 'Flows'[DaysSinceModified] < 90, `"03-Active (<90d)`", 'Flows'[DaysSinceModified] < 365, `"04-Mature (<1y)`", `"05-Stale (>1y)`")")
        (New-CalcColumnDef "TriggerCategory" "SWITCH(TRUE(), 'Flows'[TriggerType] = `"Recurrence`", `"Scheduled`", 'Flows'[TriggerType] = `"Request`", `"Manual / HTTP`", CONTAINSSTRING('Flows'[TriggerType], `"Subscription`"), `"Event-driven`", CONTAINSSTRING('Flows'[TriggerType], `"OpenApiConnection`"), `"Connector-based`", `"Other`")")
        (New-CalcColumnDef "ActionCount" "COALESCE(CALCULATE(COUNTROWS('FlowActions')), 0)" "int64")
        (New-CalcColumnDef "ComplexityBand" "SWITCH(TRUE(), 'Flows'[ActionCount] = 0, `"00-No Actions`", 'Flows'[ActionCount] <= 5, `"01-Simple`", 'Flows'[ActionCount] <= 20, `"02-Moderate`", 'Flows'[ActionCount] <= 50, `"03-Complex`", `"04-Very Complex`")")
        (New-CalcColumnDef "IsOrphaned" "ISBLANK('Flows'[CreatorDisplayName]) || 'Flows'[CreatorDisplayName] = `"`"" "boolean")
        (New-CalcColumnDef "RiskScore" "VAR Susp = IF('Flows'[State] = `"Suspended`", 3, 0) VAR Stale = IF('Flows'[DaysSinceModified] >= 90, 2, 0) VAR Orph = IF('Flows'[IsOrphaned], 3, 0) VAR Unmgd = IF('Flows'[IsManaged] = FALSE(), 1, 0) VAR NoSol = IF('Flows'[IsSolutionAware] = FALSE(), 1, 0) RETURN Susp + Stale + Orph + Unmgd + NoSol" "int64")
        (New-CalcColumnDef "RiskBand" "SWITCH(TRUE(), 'Flows'[RiskScore] = 0, `"01-None`", 'Flows'[RiskScore] <= 2, `"02-Low`", 'Flows'[RiskScore] <= 4, `"03-Medium`", `"04-High`")")
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
        # Time intelligence
        (New-MeasureDef "Flows Created MTD" "CALCULATE([Total Flows], DATESMTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created QTD" "CALCULATE([Total Flows], DATESQTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created YTD" "CALCULATE([Total Flows], DATESYTD('Calendar'[Date]))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created Last 7d" "CALCULATE([Total Flows], DATESINPERIOD('Calendar'[Date], TODAY(), -7, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created Last 30d Calendar" "CALCULATE([Total Flows], DATESINPERIOD('Calendar'[Date], TODAY(), -30, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created Last 90d" "CALCULATE([Total Flows], DATESINPERIOD('Calendar'[Date], TODAY(), -90, DAY))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows Created PYTD" "CALCULATE([Total Flows], DATESYTD(SAMEPERIODLASTYEAR('Calendar'[Date])))" "#,##0" "Time Intel")
        (New-MeasureDef "Flows YoY Change" "[Flows Created YTD] - [Flows Created PYTD]" "+#,##0;-#,##0;0" "Time Intel")
        (New-MeasureDef "Flows YoY %" "DIVIDE([Flows YoY Change], [Flows Created PYTD])" "+0.0%;-0.0%;0.0%" "Time Intel")
        (New-MeasureDef "Flows Modified Last 30d" "CALCULATE([Total Flows], DATESINPERIOD('Calendar'[Date], TODAY(), -30, DAY), USERELATIONSHIP('Calendar'[Date], 'Flows'[ModifiedDate]))" "#,##0" "Time Intel")
        # Composite resource velocity
        (New-MeasureDef "Net New Resources 30d" "[Apps Created Last 30d Calendar] + [Flows Created Last 30d Calendar]" "#,##0" "Time Intel")
        (New-MeasureDef "Daily Resource Velocity" "DIVIDE([Net New Resources 30d], 30, 0)" "0.0" "Time Intel")
        # Risk
        (New-MeasureDef "Flow Risk Score Total" "SUMX('Flows', 'Flows'[RiskScore])" "#,##0" "Risk")
        (New-MeasureDef "High Risk Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[RiskBand] = `"04-High`") + 0" "#,##0" "Risk")
        (New-MeasureDef "Medium Risk Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[RiskBand] = `"03-Medium`") + 0" "#,##0" "Risk")
        # Complexity
        (New-MeasureDef "Avg Action Count" "AVERAGE('Flows'[ActionCount])" "0.0" "Complexity")
        (New-MeasureDef "Max Action Count" "MAX('Flows'[ActionCount])" "#,##0" "Complexity")
        (New-MeasureDef "Mega Flows (>50 actions)" "CALCULATE(COUNTROWS('Flows'), 'Flows'[ActionCount] > 50) + 0" "#,##0" "Complexity")
        (New-MeasureDef "Empty Flows" "CALCULATE(COUNTROWS('Flows'), 'Flows'[ActionCount] = 0) + 0" "#,##0" "Complexity")
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
        # Calculated columns
        (New-CalcColumnDef "ConnectorKey" "'Connectors'[ConnectorId] & `"|`" & 'Connectors'[EnvironmentId]")
        (New-CalcColumnDef "RiskCategory" "SWITCH(TRUE(), CONTAINSSTRING('Connectors'[ConnectorId], `"sendhttp`") || CONTAINSSTRING('Connectors'[ConnectorId], `"webcontents`") || CONTAINSSTRING('Connectors'[ConnectorId], `"httpwebhook`"), `"04-High (HTTP)`", 'Connectors'[IsCustom] = TRUE(), `"03-Medium (Custom)`", 'Connectors'[Tier] = `"Premium`", `"02-Low (Premium)`", `"01-Low (Standard)`")")
        (New-CalcColumnDef "PublisherCategory" "SWITCH(TRUE(), 'Connectors'[Publisher] = `"Microsoft`", `"Microsoft`", 'Connectors'[IsCustom] = TRUE(), `"Custom`", 'Connectors'[Publisher] = `"`" || ISBLANK('Connectors'[Publisher]), `"Unknown`", `"Third-party`")")
    )
    partitions = @((New-CsvPartition "Connectors" @(
        @{Name="ConnectorId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="Publisher"; Type="type text"},
        @{Name="Tier"; Type="type text"}, @{Name="IsCustom"; Type="type logical"},
        @{Name="IconUri"; Type="type text"}, @{Name="CollectedAt"; Type="type datetime"}
    ) -PreTransformSteps @(
        '    Deduped = Table.Distinct(Headers, {"ConnectorId", "EnvironmentId"}),'
    )))
    measures = @(
        (New-MeasureDef "Total Connectors" "COUNTROWS('Connectors')")
        (New-MeasureDef "Custom Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[IsCustom] = TRUE())")
        (New-MeasureDef "Premium Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[Tier] = `"Premium`") + 0")
        # New governance measures
        (New-MeasureDef "Standard Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[Tier] = `"Standard`")")
        (New-MeasureDef "Unique Connector Types" "DISTINCTCOUNT('Connectors'[DisplayName])")
        (New-MeasureDef "Connector Utilization" "DIVIDE(DISTINCTCOUNT('FlowConnectionRefs'[ConnectorId]), COUNTROWS('Connectors'), 0)" "0.0%")
        (New-MeasureDef "Microsoft Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[PublisherCategory] = `"Microsoft`")" "#,##0" "Connector Mix")
        (New-MeasureDef "Third-party Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[PublisherCategory] = `"Third-party`")" "#,##0" "Connector Mix")
        (New-MeasureDef "Custom Connector Rate" "DIVIDE([Custom Connectors], [Total Connectors], 0)" "0.0%" "Connector Mix")
        (New-MeasureDef "Premium Connector Rate" "DIVIDE([Premium Connectors], [Total Connectors], 0)" "0.0%" "Connector Mix")
        (New-MeasureDef "Connectors per Env" "DIVIDE([Total Connectors], [Total Environments], 0)" "0.0" "Connector Mix")
        (New-MeasureDef "High-Risk Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[RiskCategory] = `"04-High (HTTP)`") + 0" "#,##0" "Risk")
        (New-MeasureDef "Connectors Without DLP Rule" "VAR ConnIds = VALUES('Connectors'[ConnectorId]) VAR Covered = CALCULATETABLE(VALUES('DlpConnectorRules'[ConnectorId]), ALL('DlpConnectorRules')) RETURN COUNTROWS(EXCEPT(ConnIds, Covered)) + 0" "#,##0" "DLP")
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
        (New-MeasureDef "Disabled Policies" "CALCULATE(COUNTROWS('DlpPolicies'), 'DlpPolicies'[IsEnabled] = FALSE())" "#,##0" "DLP")
        (New-MeasureDef "DLP Coverage Rate" "DIVIDE([Enabled Policies], [Total DLP Policies], 0)" "0.0%" "Governance")
        (New-MeasureDef "Tenant-scope Policies" "CALCULATE(COUNTROWS('DlpPolicies'), 'DlpPolicies'[EnvironmentScope] = `"AllEnvironments`") + 0" "#,##0" "DLP")
        (New-MeasureDef "Env-scope Policies" "CALCULATE(COUNTROWS('DlpPolicies'), 'DlpPolicies'[EnvironmentScope] <> `"AllEnvironments`") + 0" "#,##0" "DLP")
        (New-MeasureDef "Avg Connectors Per Policy" "DIVIDE([Total Connector Rules], [Total DLP Policies], 0)" "0.0" "DLP")
        (New-MeasureDef "Recent Policy Changes (30d)" "CALCULATE(COUNTROWS('DlpPolicies'), 'DlpPolicies'[LastModifiedTime] >= TODAY() - 30) + 0" "#,##0" "DLP")
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
        (New-CalcColumnDef "DateOnly" "DATE(YEAR('UsageAnalytics'[Date]), MONTH('UsageAnalytics'[Date]), DAY('UsageAnalytics'[Date]))" "dateTime")
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
        (New-ColumnDef "BaseDomain")
        (New-CalcColumnDef "ConnectorKey" "'AppConnectorRefs'[ConnectorId] & `"|`" & 'AppConnectorRefs'[EnvironmentId]")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('AppConnectorRefs'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
        (New-CalcColumnDef "IsExternalHost" "NOT(CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"microsoft`") || CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"sharepoint`") || CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"office365`") || CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"onmicrosoft`") || CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"dynamics`") || CONTAINSSTRING('AppConnectorRefs'[BaseDomain], `"azure`"))" "boolean")
    )
    partitions = @((New-CsvPartition "AppConnectorRefs" @(
        @{Name="AppId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="DataSources"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseDomain"; Type="type text"}
    ) -PreTransformSteps @(
        '    AddBaseDomain = Table.AddColumn(Headers, "BaseDomain", each try Uri.Parts(Text.Trim([EndpointUrl]))[Host] otherwise ""),'
    )))
    measures = @(
        (New-MeasureDef "Total Connector References" "COUNTROWS('AppConnectorRefs')")
        (New-MeasureDef "Distinct App Endpoints" "DISTINCTCOUNT('AppConnectorRefs'[EndpointUrl])")
        (New-MeasureDef "App Refs with Endpoints" "CALCULATE(COUNTROWS('AppConnectorRefs'), NOT(ISBLANK('AppConnectorRefs'[EndpointUrl])) && 'AppConnectorRefs'[EndpointUrl] <> `"`") + 0")
        (New-MeasureDef "App HTTP Connector Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), NOT(ISBLANK('AppConnectorRefs'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "App HTTP Raw Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), 'AppConnectorRefs'[HttpConnectorType] = `"HTTP`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "App HTTP Entra Refs" "CALCULATE(COUNTROWS('AppConnectorRefs'), 'AppConnectorRefs'[HttpConnectorType] = `"HTTP with Azure AD`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "App External Endpoints" "CALCULATE(COUNTROWS('AppConnectorRefs'), 'AppConnectorRefs'[IsExternalHost] = TRUE())" "#,##0" "HTTP Risk")
        (New-MeasureDef "App Distinct Domains" "DISTINCTCOUNT('AppConnectorRefs'[BaseDomain])" "#,##0" "HTTP Risk")
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
        (New-ColumnDef "BaseDomain")
        (New-CalcColumnDef "ConnectorKey" "'FlowActions'[ConnectorId] & `"|`" & 'FlowActions'[EnvironmentId]")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('FlowActions'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('FlowActions'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowActions'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowActions'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
        (New-CalcColumnDef "IsExternalHost" "NOT(ISBLANK('FlowActions'[BaseDomain])) && NOT(CONTAINSSTRING('FlowActions'[BaseDomain], `"microsoft`") || CONTAINSSTRING('FlowActions'[BaseDomain], `"sharepoint`") || CONTAINSSTRING('FlowActions'[BaseDomain], `"office365`") || CONTAINSSTRING('FlowActions'[BaseDomain], `"onmicrosoft`") || CONTAINSSTRING('FlowActions'[BaseDomain], `"dynamics`") || CONTAINSSTRING('FlowActions'[BaseDomain], `"azure`"))" "boolean")
    )
    partitions = @((New-CsvPartition "FlowActions" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="ActionType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}, @{Name="BaseDomain"; Type="type text"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
        '    EnsureBaseUrl = if Table.HasColumns(EnsureFlowKey, "BaseUrl") then EnsureFlowKey else Table.AddColumn(EnsureFlowKey, "BaseUrl", each ""),'
        '    AddBaseDomain = Table.AddColumn(EnsureBaseUrl, "BaseDomain", each try Uri.Parts(Text.Trim([BaseUrl]))[Host] otherwise (try Uri.Parts(Text.Trim([EndpointUrl]))[Host] otherwise "")),'
    )))
    measures = @(
        (New-MeasureDef "Total Flow Actions" "COUNTROWS('FlowActions')")
        (New-MeasureDef "Distinct Action Endpoints" "DISTINCTCOUNT('FlowActions'[EndpointUrl])")
        (New-MeasureDef "Actions with Endpoints" "CALCULATE(COUNTROWS('FlowActions'), NOT(ISBLANK('FlowActions'[EndpointUrl])) && 'FlowActions'[EndpointUrl] <> `"`") + 0")
        (New-MeasureDef "Flow HTTP Actions" "CALCULATE(COUNTROWS('FlowActions'), NOT(ISBLANK('FlowActions'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow HTTP Raw Actions" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[HttpConnectorType] = `"HTTP`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow HTTP Entra Actions" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[HttpConnectorType] = `"HTTP with Azure AD`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow HTTP Webhook Actions" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[HttpConnectorType] = `"HTTP Webhook`") + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow External Action Endpoints" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[IsExternalHost] = TRUE()) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow Internal Action Endpoints" "CALCULATE(COUNTROWS('FlowActions'), 'FlowActions'[IsExternalHost] = FALSE() && NOT(ISBLANK('FlowActions'[BaseDomain]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Flow Distinct Domains" "DISTINCTCOUNT('FlowActions'[BaseDomain])" "#,##0" "HTTP Risk")
        (New-MeasureDef "HTTP Risk Score" "[Flow HTTP Raw Actions] * 3 + [Flow HTTP Webhook Actions] * 2 + [Flow HTTP Entra Actions]" "#,##0" "Risk")
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
        (New-ColumnDef "BaseDomain")
        (New-CalcColumnDef "ConnectorKey" "'FlowTriggers'[ConnectorId] & `"|`" & 'FlowTriggers'[EnvironmentId]")
        (New-CalcColumnDef "HttpConnectorType" "SWITCH(TRUE(), CONTAINSSTRING('FlowTriggers'[ConnectorId], `"sendhttp`"), `"HTTP`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"webcontents`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"httpwithazuread`"), `"HTTP with Azure AD`", CONTAINSSTRING('FlowTriggers'[ConnectorId], `"httpwebhook`"), `"HTTP Webhook`", BLANK())")
        (New-CalcColumnDef "TriggerCategory" "SWITCH(TRUE(), 'FlowTriggers'[TriggerType] = `"Recurrence`", `"Scheduled`", 'FlowTriggers'[TriggerType] = `"Request`", `"Manual / HTTP`", CONTAINSSTRING('FlowTriggers'[TriggerType], `"Subscription`"), `"Event-driven`", CONTAINSSTRING('FlowTriggers'[TriggerType], `"OpenApiConnection`"), `"Connector-based`", `"Other`")")
    )
    partitions = @((New-CsvPartition "FlowTriggers" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowKey"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="TriggerType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}, @{Name="BaseDomain"; Type="type text"}
    ) -PreTransformSteps @(
        '    EnsureFlowKey = if Table.HasColumns(Headers, "FlowKey") then Headers else Table.AddColumn(Headers, "FlowKey", each [FlowId] & "|" & [EnvironmentId]),'
        '    EnsureBaseUrl = if Table.HasColumns(EnsureFlowKey, "BaseUrl") then EnsureFlowKey else Table.AddColumn(EnsureFlowKey, "BaseUrl", each ""),'
        '    AddBaseDomain = Table.AddColumn(EnsureBaseUrl, "BaseDomain", each try Uri.Parts(Text.Trim([BaseUrl]))[Host] otherwise (try Uri.Parts(Text.Trim([EndpointUrl]))[Host] otherwise "")),'
    )))
    measures = @(
        (New-MeasureDef "Total Flow Triggers" "COUNTROWS('FlowTriggers')")
        (New-MeasureDef "Distinct Trigger Endpoints" "DISTINCTCOUNT('FlowTriggers'[EndpointUrl])")
        (New-MeasureDef "Flow HTTP Triggers" "CALCULATE(COUNTROWS('FlowTriggers'), NOT(ISBLANK('FlowTriggers'[HttpConnectorType]))) + 0" "#,##0" "HTTP Risk")
        (New-MeasureDef "Scheduled Triggers" "CALCULATE(COUNTROWS('FlowTriggers'), 'FlowTriggers'[TriggerCategory] = `"Scheduled`")" "#,##0")
        (New-MeasureDef "Event Triggers" "CALCULATE(COUNTROWS('FlowTriggers'), 'FlowTriggers'[TriggerCategory] = `"Event-driven`")" "#,##0")
        (New-MeasureDef "Manual Triggers" "CALCULATE(COUNTROWS('FlowTriggers'), 'FlowTriggers'[TriggerCategory] = `"Manual / HTTP`")" "#,##0")
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
        (New-CalcColumnDef "ConnectorKey" "'FlowConnectionRefs'[ConnectorId] & `"|`" & 'FlowConnectionRefs'[EnvironmentId]")
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
        (New-MeasureDef "Connector Reach (Flows)" "DISTINCTCOUNT('FlowConnectionRefs'[FlowKey])" "#,##0" "Connections")
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
        (New-CalcColumnDef "CreatedDate" "IF(ISBLANK('CopilotAgents'[CreatedOn]), BLANK(), DATE(YEAR('CopilotAgents'[CreatedOn]), MONTH('CopilotAgents'[CreatedOn]), DAY('CopilotAgents'[CreatedOn])))" "dateTime")
        (New-CalcColumnDef "ModifiedDate" "IF(ISBLANK('CopilotAgents'[ModifiedOn]), BLANK(), DATE(YEAR('CopilotAgents'[ModifiedOn]), MONTH('CopilotAgents'[ModifiedOn]), DAY('CopilotAgents'[ModifiedOn])))" "dateTime")
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
        # Maturity / adoption funnel
        (New-MeasureDef "Stage 1 - Created Agents" "[Total Agents]" "#,##0" "Maturity Funnel")
        (New-MeasureDef "Stage 2 - Configured Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[TopicCount] >= 3)" "#,##0" "Maturity Funnel")
        (New-MeasureDef "Stage 3 - Published Agents" "[Published Agents]" "#,##0" "Maturity Funnel")
        (New-MeasureDef "Stage 4 - Active 30d" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[ModifiedOn] >= TODAY() - 30)" "#,##0" "Maturity Funnel")
        (New-MeasureDef "Empty Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[TopicCount] = 0 && 'CopilotAgents'[KnowledgeSourceCount] = 0)" "#,##0" "Maturity Funnel")
        (New-MeasureDef "Auth Risk Agents" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[AuthenticationMode] IN {`"Unspecified`", `"None`", `"Open`"})" "#,##0" "Risk")
        (New-MeasureDef "Avg Topics Per Agent" "AVERAGE('CopilotAgents'[TopicCount])" "0.0" "Component Metrics")
        (New-MeasureDef "Avg Knowledge Per Agent" "AVERAGE('CopilotAgents'[KnowledgeSourceCount])" "0.0" "Component Metrics")
        (New-MeasureDef "Knowledge Coverage" "DIVIDE(CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[KnowledgeSourceCount] > 0), [Total Agents], 0)" "0.0%" "Component Metrics")
        (New-MeasureDef "Funnel: Configured Rate" "DIVIDE([Stage 2 - Configured Agents], [Stage 1 - Created Agents], 0)" "0.0%" "Maturity Funnel")
        (New-MeasureDef "Funnel: Publish Rate" "DIVIDE([Stage 3 - Published Agents], [Stage 2 - Configured Agents], 0)" "0.0%" "Maturity Funnel")
        (New-MeasureDef "Funnel: Activation Rate" "DIVIDE([Stage 4 - Active 30d], [Stage 3 - Published Agents], 0)" "0.0%" "Maturity Funnel")
        (New-MeasureDef "Agents Created Last 30d" "CALCULATE(COUNTROWS('CopilotAgents'), 'CopilotAgents'[CreatedOn] >= TODAY() - 30)" "#,##0" "Time Intel")
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

# --- Table 14: Calendar (Date dimension - calculated table) ---

$tCalendar = [ordered]@{
    name = "Calendar"; lineageTag = (New-Guid)
    isHidden = $false
    columns = @(
        (New-ColumnDef "Date" "dateTime" "none" "yyyy-MM-dd" $true)
        (New-CalcColumnDef "Year" "YEAR('Calendar'[Date])" "int64")
        (New-CalcColumnDef "Quarter" "`"Q`" & FORMAT('Calendar'[Date], `"q`")" "string")
        (New-CalcColumnDef "Month" "FORMAT('Calendar'[Date], `"MMM`")" "string")
        (New-CalcColumnDef "MonthNum" "MONTH('Calendar'[Date])" "int64")
        (New-CalcColumnDef "MonthYear" "FORMAT('Calendar'[Date], `"yyyy-MM`")" "string")
        (New-CalcColumnDef "MonthYearLabel" "FORMAT('Calendar'[Date], `"MMM yyyy`")" "string")
        (New-CalcColumnDef "WeekStart" "'Calendar'[Date] - WEEKDAY('Calendar'[Date], 2) + 1" "dateTime")
        (New-CalcColumnDef "DayOfWeek" "FORMAT('Calendar'[Date], `"dddd`")" "string")
        (New-CalcColumnDef "DayOfWeekNum" "WEEKDAY('Calendar'[Date], 2)" "int64")
        (New-CalcColumnDef "IsWeekend" "WEEKDAY('Calendar'[Date], 2) >= 6" "boolean")
        (New-CalcColumnDef "IsLast7Days" "'Calendar'[Date] >= TODAY() - 7" "boolean")
        (New-CalcColumnDef "IsLast30Days" "'Calendar'[Date] >= TODAY() - 30" "boolean")
        (New-CalcColumnDef "IsLast90Days" "'Calendar'[Date] >= TODAY() - 90" "boolean")
        (New-CalcColumnDef "IsCurrentMonth" "YEAR('Calendar'[Date]) = YEAR(TODAY()) && MONTH('Calendar'[Date]) = MONTH(TODAY())" "boolean")
        (New-CalcColumnDef "IsCurrentYear" "YEAR('Calendar'[Date]) = YEAR(TODAY())" "boolean")
    )
    partitions = @(
        [ordered]@{
            name = "Calendar-Partition"
            mode = "import"
            source = [ordered]@{
                type = "calculated"
                expression = "CALENDAR(DATE(YEAR(MIN('Apps'[CreatedTime])) - 1, 1, 1), DATE(YEAR(TODAY()) + 1, 12, 31))"
            }
        }
    )
    measures = @(
        (New-MeasureDef "Selected Period Days" "DATEDIFF(MIN('Calendar'[Date]), MAX('Calendar'[Date]), DAY)" "#,##0" "Time Intel")
    )
}

# --- Table 15: AppPermissions ---

$tAppPermissions = [ordered]@{
    name = "AppPermissions"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "AppId" "string" "none" $null $false $true)
        (New-ColumnDef "AppName")
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "PrincipalId")
        (New-ColumnDef "PrincipalType")
        (New-ColumnDef "PrincipalDisplay")
        (New-ColumnDef "PrincipalEmail")
        (New-ColumnDef "RoleName")
        (New-CalcColumnDef "PrincipalCategory" "SWITCH(TRUE(), 'AppPermissions'[PrincipalType] = `"Tenant`", `"Tenant`", 'AppPermissions'[PrincipalType] = `"Group`", `"Group`", 'AppPermissions'[PrincipalType] = `"User`", `"User`", `"Other`")")
        (New-CalcColumnDef "AccessLevel" "SWITCH(TRUE(), 'AppPermissions'[RoleName] = `"CanEdit`" || 'AppPermissions'[RoleName] = `"Owner`" || 'AppPermissions'[RoleName] = `"CanEditWithReshare`", `"Edit`", 'AppPermissions'[RoleName] = `"CanView`" || 'AppPermissions'[RoleName] = `"CanViewWithReshare`", `"View`", `"Other`")")
    )
    partitions = @((New-CsvPartitionOptional "AppPermissions" @(
        @{Name="AppId"; Type="type text"}, @{Name="AppName"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"}, @{Name="PrincipalId"; Type="type text"},
        @{Name="PrincipalType"; Type="type text"}, @{Name="PrincipalDisplay"; Type="type text"},
        @{Name="PrincipalEmail"; Type="type text"}, @{Name="RoleName"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total App Permissions" "COUNTROWS('AppPermissions')" "#,##0" "Permissions")
        (New-MeasureDef "App User Permissions" "CALCULATE(COUNTROWS('AppPermissions'), 'AppPermissions'[PrincipalCategory] = `"User`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "App Group Permissions" "CALCULATE(COUNTROWS('AppPermissions'), 'AppPermissions'[PrincipalCategory] = `"Group`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "App Tenant Permissions" "CALCULATE(COUNTROWS('AppPermissions'), 'AppPermissions'[PrincipalCategory] = `"Tenant`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "Tenant-Shared App Count" "CALCULATE(DISTINCTCOUNT('AppPermissions'[AppId]), 'AppPermissions'[PrincipalCategory] = `"Tenant`")" "#,##0" "Permissions")
        (New-MeasureDef "Group-Shared App Count" "CALCULATE(DISTINCTCOUNT('AppPermissions'[AppId]), 'AppPermissions'[PrincipalCategory] = `"Group`")" "#,##0" "Permissions")
        (New-MeasureDef "App Co-Owners" "CALCULATE(COUNTROWS('AppPermissions'), 'AppPermissions'[AccessLevel] = `"Edit`")" "#,##0" "Permissions")
        (New-MeasureDef "App Editor-to-Viewer Ratio" "DIVIDE([App Co-Owners], CALCULATE(COUNTROWS('AppPermissions'), 'AppPermissions'[AccessLevel] = `"View`"), 0)" "0.0" "Permissions")
        (New-MeasureDef "Avg Editors Per App" "DIVIDE([App Co-Owners], DISTINCTCOUNT('AppPermissions'[AppId]), 0)" "0.0" "Permissions")
    )
}

# --- Table 16: FlowPermissions ---

$tFlowPermissions = [ordered]@{
    name = "FlowPermissions"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId" "string" "none" $null $false $true)
        (New-ColumnDef "FlowName")
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "PrincipalId")
        (New-ColumnDef "PrincipalType")
        (New-ColumnDef "PrincipalDisplay")
        (New-ColumnDef "PrincipalEmail")
        (New-ColumnDef "RoleName")
        (New-CalcColumnDef "PrincipalCategory" "SWITCH(TRUE(), 'FlowPermissions'[PrincipalType] = `"Tenant`", `"Tenant`", 'FlowPermissions'[PrincipalType] = `"Group`", `"Group`", 'FlowPermissions'[PrincipalType] = `"User`", `"User`", `"Other`")")
        (New-CalcColumnDef "AccessLevel" "SWITCH(TRUE(), 'FlowPermissions'[RoleName] = `"Owner`" || 'FlowPermissions'[RoleName] = `"CanEdit`", `"Edit`", 'FlowPermissions'[RoleName] = `"CanView`", `"View`", `"Other`")")
    )
    partitions = @((New-CsvPartitionOptional "FlowPermissions" @(
        @{Name="FlowId"; Type="type text"}, @{Name="FlowName"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"}, @{Name="PrincipalId"; Type="type text"},
        @{Name="PrincipalType"; Type="type text"}, @{Name="PrincipalDisplay"; Type="type text"},
        @{Name="PrincipalEmail"; Type="type text"}, @{Name="RoleName"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Flow Permissions" "COUNTROWS('FlowPermissions')" "#,##0" "Permissions")
        (New-MeasureDef "Flow User Permissions" "CALCULATE(COUNTROWS('FlowPermissions'), 'FlowPermissions'[PrincipalCategory] = `"User`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "Flow Group Permissions" "CALCULATE(COUNTROWS('FlowPermissions'), 'FlowPermissions'[PrincipalCategory] = `"Group`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "Flow Tenant Permissions" "CALCULATE(COUNTROWS('FlowPermissions'), 'FlowPermissions'[PrincipalCategory] = `"Tenant`") + 0" "#,##0" "Permissions")
        (New-MeasureDef "Flow Co-Owners" "CALCULATE(COUNTROWS('FlowPermissions'), 'FlowPermissions'[AccessLevel] = `"Edit`")" "#,##0" "Permissions")
        (New-MeasureDef "Avg Co-Owners Per Flow" "DIVIDE([Flow Co-Owners], DISTINCTCOUNT('FlowPermissions'[FlowId]), 0)" "0.0" "Permissions")
    )
}

# --- Table 17: Errors (data-pipeline / collection telemetry) ---

$tErrors = [ordered]@{
    name = "Errors"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "EnvironmentName")
        (New-ColumnDef "Phase")
        (New-ColumnDef "Error")
        (New-ColumnDef "Timestamp" "dateTime" "none" "yyyy-MM-dd HH:mm")
        (New-CalcColumnDef "ErrorType" "SWITCH(TRUE(), CONTAINSSTRING('Errors'[Error], `"401`") || CONTAINSSTRING('Errors'[Error], `"403`"), `"Auth`", CONTAINSSTRING('Errors'[Error], `"429`"), `"Throttle`", CONTAINSSTRING('Errors'[Error], `"timeout`") || CONTAINSSTRING('Errors'[Error], `"Timeout`"), `"Timeout`", CONTAINSSTRING('Errors'[Error], `"500`") || CONTAINSSTRING('Errors'[Error], `"502`") || CONTAINSSTRING('Errors'[Error], `"503`"), `"Server Error`", `"Other`")")
    )
    partitions = @((New-CsvPartitionOptional "Errors" @(
        @{Name="EnvironmentId"; Type="type text"}, @{Name="EnvironmentName"; Type="type text"},
        @{Name="Phase"; Type="type text"}, @{Name="Error"; Type="type text"},
        @{Name="Timestamp"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Errors" "COUNTROWS('Errors') + 0" "#,##0" "Data Health")
        (New-MeasureDef "Auth Errors" "CALCULATE(COUNTROWS('Errors'), 'Errors'[ErrorType] = `"Auth`") + 0" "#,##0" "Data Health")
        (New-MeasureDef "Throttle Errors" "CALCULATE(COUNTROWS('Errors'), 'Errors'[ErrorType] = `"Throttle`") + 0" "#,##0" "Data Health")
        (New-MeasureDef "Timeout Errors" "CALCULATE(COUNTROWS('Errors'), 'Errors'[ErrorType] = `"Timeout`") + 0" "#,##0" "Data Health")
        (New-MeasureDef "Server Errors" "CALCULATE(COUNTROWS('Errors'), 'Errors'[ErrorType] = `"Server Error`") + 0" "#,##0" "Data Health")
        (New-MeasureDef "Affected Environments" "DISTINCTCOUNT('Errors'[EnvironmentId])" "#,##0" "Data Health")
        (New-MeasureDef "Last Error Time" "FORMAT(MAX('Errors'[Timestamp]), `"yyyy-MM-dd HH:mm`")" $null "Data Health")
    )
}

# --- Table 18: Connections ---

$tConnections = [ordered]@{
    name = "Connections"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ConnectionKey" "string" "none" -IsKey $true)
        (New-ColumnDef "ConnectionId" "string" "none")
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
        # Calculated columns
        (New-CalcColumnDef "ConnectorRefKey" "'Connections'[ConnectorId] & `"|`" & 'Connections'[EnvironmentId]")
        (New-CalcColumnDef "CreatedDate" "DATE(YEAR('Connections'[CreatedTime]), MONTH('Connections'[CreatedTime]), DAY('Connections'[CreatedTime]))" "dateTime")
        (New-CalcColumnDef "ConnectionAgeDays" "IF(ISBLANK('Connections'[CreatedTime]), BLANK(), DATEDIFF('Connections'[CreatedTime], TODAY(), DAY))" "int64")
        (New-CalcColumnDef "AgeBand" "SWITCH(TRUE(), 'Connections'[ConnectionAgeDays] < 30, `"01-New (<30d)`", 'Connections'[ConnectionAgeDays] < 180, `"02-Active (<180d)`", 'Connections'[ConnectionAgeDays] < 365, `"03-Mature (<1y)`", `"04-Old (>1y)`")")
        (New-CalcColumnDef "IsHealthy" "'Connections'[Status] = `"Connected`"" "boolean")
    )
    partitions = @((New-CsvPartition "Connections" @(
        @{Name="ConnectionKey"; Type="type text"}, @{Name="ConnectionId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"},
        @{Name="EnvironmentId"; Type="type text"}, @{Name="EnvironmentName"; Type="type text"},
        @{Name="DisplayName"; Type="type text"}, @{Name="ConnectionUrl"; Type="type text"},
        @{Name="CreatedByObjectId"; Type="type text"}, @{Name="CreatedByName"; Type="type text"},
        @{Name="CreatedByEmail"; Type="type text"}, @{Name="CreatedTime"; Type="type datetime"},
        @{Name="Status"; Type="type text"}, @{Name="IsShared"; Type="type logical"},
        @{Name="CollectedAt"; Type="type datetime"}
    ) @(
        '    AddKey = Table.AddColumn(Headers, "ConnectionKey", each [ConnectionId] & "|" & [EnvironmentId]),'
        '    Deduped = Table.Distinct(AddKey, {"ConnectionKey"}),'
    )))
    measures = @(
        (New-MeasureDef "Total Connections" "COUNTROWS('Connections')" "#,##0" "Connections")
        (New-MeasureDef "Shared Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[IsShared] = TRUE())" "#,##0" "Connections")
        (New-MeasureDef "Active Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[Status] = `"Connected`")" "#,##0" "Connections")
        (New-MeasureDef "Unique Connection Creators" "DISTINCTCOUNT('Connections'[CreatedByObjectId])" "#,##0" "Connections")
        (New-MeasureDef "Connector Types Used" "DISTINCTCOUNT('Connections'[ConnectorId])" "#,##0" "Connections")
        (New-MeasureDef "Failed Connections" "CALCULATE(COUNTROWS('Connections'), NOT('Connections'[Status] IN {`"Connected`", `"Connecting`"}))" "#,##0" "Connections")
        (New-MeasureDef "Failed Connection Rate" "DIVIDE([Failed Connections], [Total Connections], 0)" "0.0%" "Connections")
        (New-MeasureDef "Avg Connection Age (days)" "AVERAGE('Connections'[ConnectionAgeDays])" "0" "Connections")
        (New-MeasureDef "Stale Connections (>180d)" "CALCULATE(COUNTROWS('Connections'), 'Connections'[ConnectionAgeDays] > 180)" "#,##0" "Connections")
        (New-MeasureDef "Sharing Rate (Connections)" "DIVIDE([Shared Connections], [Total Connections], 0)" "0.0%" "Connections")
        (New-MeasureDef "Connections Created Last 30d" "CALCULATE(COUNTROWS('Connections'), 'Connections'[CreatedTime] >= TODAY() - 30)" "#,##0" "Connections")
        (New-MeasureDef "Orphaned Connections" "VAR Used = CALCULATETABLE(VALUES('FlowConnectionRefs'[ConnectionName]), ALL('FlowConnectionRefs')) RETURN COUNTROWS(FILTER(VALUES('Connections'[ConnectionId]), NOT('Connections'[ConnectionId] IN Used)))" "#,##0" "Connections")
    )
}

# --- Build model.bim ---

# Inactive relationship helper — Calendar to dual date columns on Apps/Flows
function New-InactiveRelationshipDef {
    param([string]$Name, [string]$FromTable, [string]$FromColumn, [string]$ToTable, [string]$ToColumn)
    [ordered]@{
        name = $Name; fromTable = $FromTable; fromColumn = $FromColumn
        toTable = $ToTable; toColumn = $ToColumn
        crossFilteringBehavior = "oneDirection"
        fromCardinality = "many"; toCardinality = "one"
        isActive = $false
    }
}

$modelBim = [ordered]@{
    compatibilityLevel = 1567
    model = [ordered]@{
        culture = "en-US"
        defaultPowerBIDataSourceVersion = "powerBI_V3"
        sourceQueryCulture = "en-US"
        tables = @($tEnvironments, $tApps, $tFlows, $tConnectors,
                    $tDlpPolicies, $tDlpRules, $tUsage, $tAppConnRefs, $tFlowActions, $tFlowTriggers, $tFlowConnRefs,
                    $tCopilotAgents, $tCopilotComponents,
                    $tCalendar, $tAppPermissions, $tFlowPermissions, $tErrors,
                    $tConnections)
        relationships = @(
            # Existing relationships
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
            # NEW: Calendar relationships (active = CreatedDate, inactive = ModifiedDate, PublishedDate)
            (New-RelationshipDef "rel_Cal_AppCreated" "Apps" "CreatedDate" "Calendar" "Date")
            (New-RelationshipDef "rel_Cal_FlowCreated" "Flows" "CreatedDate" "Calendar" "Date")
            (New-RelationshipDef "rel_Cal_Usage" "UsageAnalytics" "DateOnly" "Calendar" "Date")
            (New-RelationshipDef "rel_Cal_AgentCreated" "CopilotAgents" "CreatedDate" "Calendar" "Date")
            (New-RelationshipDef "rel_Cal_ConnectionCreated" "Connections" "CreatedDate" "Calendar" "Date")
            (New-InactiveRelationshipDef "rel_Cal_AppModified" "Apps" "ModifiedDate" "Calendar" "Date")
            (New-InactiveRelationshipDef "rel_Cal_FlowModified" "Flows" "ModifiedDate" "Calendar" "Date")
            (New-InactiveRelationshipDef "rel_Cal_AppPublished" "Apps" "PublishedDate" "Calendar" "Date")
            (New-InactiveRelationshipDef "rel_Cal_AgentModified" "CopilotAgents" "ModifiedDate" "Calendar" "Date")
            # NEW: Permissions relationships
            (New-RelationshipDef "rel_AppPerms_Apps" "AppPermissions" "AppId" "Apps" "AppId")
            (New-RelationshipDef "rel_FlowPerms_Env" "FlowPermissions" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_AppPerms_Env" "AppPermissions" "EnvironmentId" "Environments" "EnvironmentId")
            # NEW: Errors relationship
            (New-RelationshipDef "rel_Errors_Env" "Errors" "EnvironmentId" "Environments" "EnvironmentId")
            # NEW: ConnectorKey relationships (cross-env connector dim) — INACTIVE to
            # avoid ambiguous paths to Environments. The active path is via Apps/Flows/
            # Connections (which already carry EnvironmentId). Measures that want to
            # filter through Connectors properties (Tier, Publisher, RiskCategory) can
            # call USERELATIONSHIP to enable the connector-side path on demand.
            (New-InactiveRelationshipDef "rel_AppConnRefs_Connectors" "AppConnectorRefs" "ConnectorKey" "Connectors" "ConnectorKey")
            (New-InactiveRelationshipDef "rel_FlowActions_Connectors" "FlowActions" "ConnectorKey" "Connectors" "ConnectorKey")
            (New-InactiveRelationshipDef "rel_FlowTriggers_Connectors" "FlowTriggers" "ConnectorKey" "Connectors" "ConnectorKey")
            (New-InactiveRelationshipDef "rel_Connections_Connectors" "Connections" "ConnectorRefKey" "Connectors" "ConnectorKey")
        )
        expressions = @(
            [ordered]@{
                name = "CsvFolderPath"
                kind = "m"
                expression = @("`"$($CsvPath -replace '\\', '\\')`" meta [IsParameterQuery=true, Type=`"Text`", IsParameterQueryRequired=true]")
            }
        )
        annotations = @(
            @{ name = "PBI_QueryOrder"; value = "[`"Environments`",`"Apps`",`"Flows`",`"Connectors`",`"DlpPolicies`",`"DlpConnectorRules`",`"UsageAnalytics`",`"AppConnectorRefs`",`"FlowActions`",`"FlowTriggers`",`"FlowConnectionRefs`",`"CopilotAgents`",`"CopilotComponents`",`"AppPermissions`",`"FlowPermissions`",`"Errors`",`"Connections`",`"Calendar`"]" }
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

# --- Custom theme: Power Platform Governance ---
# Semantic colors: red=high risk, amber=medium, green=healthy, navy primary, gray neutrals.
$customThemeJson = @'
{
  "name": "PowerPlatformGovernance",
  "dataColors": [
    "#0078D4",
    "#107C10",
    "#FFB900",
    "#D13438",
    "#5C2D91",
    "#00B294",
    "#E81123",
    "#0099BC",
    "#7FBA00",
    "#F7630C",
    "#A80000",
    "#5D5A58"
  ],
  "background": "#FFFFFF",
  "foreground": "#252423",
  "tableAccent": "#0078D4",
  "good": "#107C10",
  "neutral": "#FFB900",
  "bad": "#D13438",
  "maximum": "#D13438",
  "center": "#FFB900",
  "minimum": "#107C10",
  "null": "#E1DFDD",
  "textClasses": {
    "title": { "color": "#252423", "fontFace": "Segoe UI Semibold", "fontSize": 16 },
    "header": { "color": "#252423", "fontFace": "Segoe UI Semibold", "fontSize": 14 },
    "label": { "color": "#252423", "fontFace": "Segoe UI", "fontSize": 12 },
    "callout": { "color": "#0078D4", "fontFace": "Segoe UI Semibold", "fontSize": 28 }
  },
  "visualStyles": {
    "*": {
      "*": {
        "background": [{ "show": true, "color": { "solid": { "color": "#FFFFFF" } }, "transparency": 0 }],
        "border": [{ "show": true, "color": { "solid": { "color": "#E1DFDD" } }, "radius": 4 }],
        "title": [{ "show": true, "fontColor": { "solid": { "color": "#252423" } }, "fontSize": 12, "alignment": "left" }]
      }
    },
    "card": {
      "*": {
        "labels": [{ "color": { "solid": { "color": "#0078D4" } }, "fontSize": 24, "fontFamily": "Segoe UI Semibold" }],
        "categoryLabels": [{ "color": { "solid": { "color": "#605E5C" } }, "fontSize": 11 }]
      }
    },
    "kpi": {
      "*": {
        "indicator": [{ "color": { "solid": { "color": "#0078D4" } }, "fontSize": 28 }]
      }
    },
    "tableEx": {
      "*": {
        "grid": [{ "gridVertical": true, "gridHorizontal": true, "gridVerticalColor": { "solid": { "color": "#E1DFDD" } }, "gridHorizontalColor": { "solid": { "color": "#E1DFDD" } } }],
        "columnHeaders": [{ "fontColor": { "solid": { "color": "#FFFFFF" } }, "backColor": { "solid": { "color": "#0078D4" } }, "fontFamily": "Segoe UI Semibold" }]
      }
    },
    "pivotTable": {
      "*": {
        "columnHeaders": [{ "fontColor": { "solid": { "color": "#FFFFFF" } }, "backColor": { "solid": { "color": "#0078D4" } } }]
      }
    },
    "page": {
      "*": {
        "background": [{ "color": { "solid": { "color": "#F3F2F1" } }, "transparency": 0 }]
      }
    }
  }
}
'@

# Write theme file into the report's StaticResources
$themesDir = "$reportDir/StaticResources/RegisteredResources"
if (-not (Test-Path $themesDir)) { New-Item -ItemType Directory -Path $themesDir -Force | Out-Null }
$absThemePath = Join-Path (Resolve-Path $themesDir).Path "PowerPlatformGovernance.json"
[System.IO.File]::WriteAllText($absThemePath, $customThemeJson, [System.Text.UTF8Encoding]::new($false))

$reportJsonContent = @'
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
  "layoutOptimization": "None",
  "themeCollection": {
    "baseTheme": {
      "name": "CY24SU10",
      "reportVersionAtImport": "5.55",
      "type": "SharedResources"
    },
    "customTheme": {
      "name": "PowerPlatformGovernance",
      "type": "RegisteredResources"
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
    },
    {
      "name": "RegisteredResources",
      "type": "RegisteredResources",
      "items": [
        {
          "name": "PowerPlatformGovernance",
          "path": "PowerPlatformGovernance.json",
          "type": "CustomTheme"
        }
      ]
    }
  ],
  "settings": {
    "useStylableVisualContainerHeader": true,
    "defaultDrillFilterOtherVisuals": true,
    "useEnhancedTooltips": true
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

$pageNames = @(
    # Tier 1: Command Center
    "executive",
    # Tier 2: Domain Governance
    "environments", "apps", "flows", "agents", "connectors-dlp", "endpoints",
    # Tier 3: Risk & Intelligence
    "risk", "makers", "connections",
    # NEW Tier 3 expansion
    "trends", "compliance", "sharing-perms", "http-risk", "connector-heatmap",
    "pareto", "agent-funnel", "premium", "capacity", "whats-new", "data-health",
    "glossary",
    # Tier 4: Detail / Drill-Through
    "env-details", "app-details", "flow-details", "agent-details",
    "dlp-details", "connector-details", "maker-details"
)
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
    # --- NEW PAGE: Trends & Velocity ---
    trends = @{
        displayName = "Trends & Velocity"
        visuals = @(
            (New-TextBoxVisual "txtTrendsHdr" 20 10 1220 30 10 "Adoption Trends &amp; Resource Velocity" "18" "#252423" $true)
            # KPI band — velocity gauges and YoY
            (New-CardVisual "cardTrApps30" 20 60 145 90 100 "Apps" "Apps Created Last 30d Calendar" "Apps · last 30d")
            (New-CardVisual "cardTrFlows30" 180 60 145 90 200 "Flows" "Flows Created Last 30d Calendar" "Flows · last 30d")
            (New-CardVisual "cardTrAgents30" 340 60 145 90 300 "CopilotAgents" "Agents Created Last 30d" "Agents · last 30d")
            (New-CardVisual "cardTrVelocity" 500 60 145 90 400 "Flows" "Daily Resource Velocity" "Daily velocity")
            (New-CardVisual "cardTrAppsYoY" 660 60 145 90 500 "Apps" "Apps YoY %" "Apps YoY")
            (New-CardVisual "cardTrFlowsYoY" 820 60 145 90 600 "Flows" "Flows YoY %" "Flows YoY")
            (New-CardVisual "cardTrYTD" 980 60 145 90 700 "Apps" "Apps Created YTD" "Apps YTD")
            (New-CardVisual "cardTrFlYTD" 1140 60 100 90 800 "Flows" "Flows Created YTD" "Flows YTD")
            # Charts: monthly creation trend
            (New-AreaChartVisual "areaAppsByMonth" 20 165 600 230 1000 "Calendar" "MonthYearLabel" "Apps" "Total Apps" "Apps Created by Month")
            (New-AreaChartVisual "areaFlowsByMonth" 640 165 600 230 1100 "Calendar" "MonthYearLabel" "Flows" "Total Flows" "Flows Created by Month")
            # Day-of-week heatmap
            (New-MatrixVisual "matrixDoW" 20 410 600 220 2000 `
                @(@{Table="Calendar"; Column="DayOfWeek"}) `
                @(@{Table="Apps"; Measure="Total Apps"}, @{Table="Flows"; Measure="Total Flows"}) `
                "Creation by Day of Week")
            # Agents momentum
            (New-LineChartVisual "lineAgentsTime" 640 410 600 220 2100 "Calendar" "MonthYearLabel" "CopilotAgents" "Total Agents" "Agent Adoption by Month")
            # Recent items
            (New-TableVisual "tblTrendsLatest" 20 645 1220 65 3000 "Apps" @("DisplayName","OwnerDisplayName","CreatedTime","EnvironmentName") "Latest Apps")
        )
    }
    # --- NEW PAGE: Compliance Heatmap ---
    compliance = @{
        displayName = "Compliance Heatmap"
        visuals = @(
            (New-TextBoxVisual "txtCompHdr" 20 10 1220 30 10 "Compliance Heatmap by Environment" "18" "#252423" $true)
            # KPI strip — overall scores
            (New-MultiKpiCardVisual "kpiCompOverall" 20 60 1220 110 50 @(
                @{Table="Environments"; Measure="Governance Score v2"},
                @{Table="Environments"; Measure="Env Security Rate"},
                @{Table="DlpPolicies"; Measure="DLP Coverage Rate"},
                @{Table="Environments"; Measure="Overall Solution Coverage"},
                @{Table="Environments"; Measure="Shadow IT Rate"}
            ) "Tenant Governance Composite")
            # Risk matrix env x dim — wide row data
            ,(New-MatrixVisual "matrixComp" 20 185 1220 350 1000 `
                @(@{Table="Environments"; Column="DisplayName"}) `
                @(@{Table="Environments"; Measure="Unsecured Environments"},
                  @{Table="Apps"; Measure="Orphaned Apps"},
                  @{Table="Flows"; Measure="Orphaned Flows"},
                  @{Table="Apps"; Measure="Stale Apps (90d)"},
                  @{Table="Flows"; Measure="Stale Flows (90d)"},
                  @{Table="Flows"; Measure="Suspended Flows"},
                  @{Table="Flows"; Measure="Unmanaged Flows"},
                  @{Table="Apps"; Measure="Tenant-wide Shared Apps"},
                  @{Table="FlowActions"; Measure="HTTP Risk Score"},
                  @{Table="Apps"; Measure="Premium API Apps"}) `
                "Risk Surface · Env x Dimension")
            # Decomposition tree for risk drill-down
            (New-DecompositionTreeVisual "decompRisk" 20 545 800 165 2000 `
                "Apps" "App Risk Score Total" `
                @(@{Table="Environments"; Column="EnvironmentType"},
                  @{Table="Environments"; Column="DisplayName"},
                  @{Table="Apps"; Column="OwnerDisplayName"},
                  @{Table="Apps"; Column="RiskBand"}) `
                "Risk Decomposition")
            (New-CardVisual "cardCompHighApps" 840 545 200 80 3000 "Apps" "High Risk Apps" "High-risk Apps")
            (New-CardVisual "cardCompHighFlows" 1050 545 190 80 3100 "Flows" "High Risk Flows" "High-risk Flows")
            (New-CardVisual "cardCompMedApps" 840 630 200 80 3200 "Apps" "Medium Risk Apps" "Med-risk Apps")
            (New-CardVisual "cardCompMedFlows" 1050 630 190 80 3300 "Flows" "Medium Risk Flows" "Med-risk Flows")
        )
    }
    # --- NEW PAGE: Sharing & Permissions ---
    "sharing-perms" = @{
        displayName = "Sharing & Permissions"
        visuals = @(
            (New-TextBoxVisual "txtShareHdr" 20 10 1220 30 10 "Sharing &amp; Permissions Risk" "18" "#252423" $true)
            (New-CardVisual "cardShareTotalAppPerms" 20 60 145 90 100 "AppPermissions" "Total App Permissions" "App Perms")
            (New-CardVisual "cardShareTotalFlowPerms" 180 60 145 90 200 "FlowPermissions" "Total Flow Permissions" "Flow Perms")
            (New-CardVisual "cardShareTenantApps" 340 60 145 90 300 "AppPermissions" "Tenant-Shared App Count" "Tenant-shared Apps")
            (New-CardVisual "cardShareGroupApps" 500 60 145 90 400 "AppPermissions" "Group-Shared App Count" "Group-shared Apps")
            (New-CardVisual "cardShareCoOwners" 660 60 145 90 500 "AppPermissions" "Avg Editors Per App" "Avg Editors / App")
            (New-CardVisual "cardShareWideApps" 820 60 145 90 600 "Apps" "Tenant-wide Shared Apps" "Tenant-wide Apps")
            (New-CardVisual "cardShareMaxBreadth" 980 60 145 90 700 "Apps" "Max Sharing Breadth" "Max sharing")
            (New-CardVisual "cardShareAvgBreadth" 1140 60 100 90 800 "Apps" "Avg Sharing Breadth" "Avg sharing")
            # Charts
            (New-DonutVisual "donutShareBand" 20 165 400 260 1000 "Apps" "SharingRiskBand" "Total Apps" "Apps by Sharing Band")
            (New-StackedColumnChartVisual "stackedSharePrincType" 440 165 400 260 1100 "AppPermissions" "PrincipalCategory" "AppPermissions" "AccessLevel" "AppPermissions" "Total App Permissions" "Principal × Access (Apps)")
            (New-BarChartVisual "barTopSharedApps" 860 165 380 260 1200 "Apps" "DisplayName" "Total Shared Users" "Top Shared Apps")
            # Detail tables
            (New-TableVisual "tblShareApps" 20 440 600 270 2000 "AppPermissions" @("AppName","PrincipalDisplay","PrincipalType","RoleName","AccessLevel") "App Permission Details")
            (New-TableVisual "tblShareFlows" 640 440 600 270 2100 "FlowPermissions" @("FlowName","PrincipalDisplay","PrincipalType","RoleName","AccessLevel") "Flow Permission Details")
        )
    }
    # --- NEW PAGE: HTTP Endpoint Risk ---
    "http-risk" = @{
        displayName = "HTTP Endpoint Risk"
        visuals = @(
            (New-TextBoxVisual "txtHttpHdr" 20 10 1220 30 10 "HTTP / External Endpoint Risk" "18" "#252423" $true)
            (New-CardVisual "cardHttpScore" 20 60 145 90 100 "FlowActions" "HTTP Risk Score" "HTTP Risk Score")
            (New-CardVisual "cardHttpRaw" 180 60 145 90 200 "FlowActions" "Flow HTTP Raw Actions" "HTTP Raw")
            (New-CardVisual "cardHttpEntra" 340 60 145 90 300 "FlowActions" "Flow HTTP Entra Actions" "HTTP Entra")
            (New-CardVisual "cardHttpHook" 500 60 145 90 400 "FlowActions" "Flow HTTP Webhook Actions" "Webhook")
            (New-CardVisual "cardHttpExternal" 660 60 145 90 500 "FlowActions" "Flow External Action Endpoints" "External Endpoints")
            (New-CardVisual "cardHttpDomains" 820 60 145 90 600 "FlowActions" "Flow Distinct Domains" "Distinct Domains")
            (New-CardVisual "cardHttpAppExt" 980 60 145 90 700 "AppConnectorRefs" "App External Endpoints" "App External")
            (New-CardVisual "cardHttpAppDomains" 1140 60 100 90 800 "AppConnectorRefs" "App Distinct Domains" "App Domains")
            # Charts
            (New-TreemapVisual "tmHttpDomains" 20 165 600 250 1000 "FlowActions" "BaseDomain" "FlowActions" "Total Flow Actions" "Flow Endpoint Domains")
            (New-DonutVisual "donutHttpExternal" 640 165 280 250 1100 "FlowActions" "IsExternalHost" "Total Flow Actions" "External vs Internal")
            (New-DonutVisual "donutHttpType" 940 165 300 250 1200 "FlowActions" "HttpConnectorType" "Total Flow Actions" "HTTP Connector Mix")
            # Suspicious endpoint table
            (New-TableVisual "tblHttpExt" 20 430 1220 280 2000 "FlowActions" @("BaseDomain","BaseUrl","HttpConnectorType","ConnectorId","Name","FlowKey") "External / HTTP Action Details")
        )
    }
    # --- NEW PAGE: Connector Heatmap ---
    "connector-heatmap" = @{
        displayName = "Connector Heatmap"
        visuals = @(
            (New-TextBoxVisual "txtCnHmHdr" 20 10 1220 30 10 "Connector Adoption Heatmap" "18" "#252423" $true)
            (New-CardVisual "cardCnHmTotal" 20 60 145 90 100 "Connectors" "Total Connectors" "Total")
            (New-CardVisual "cardCnHmCustom" 180 60 145 90 200 "Connectors" "Custom Connectors" "Custom")
            (New-CardVisual "cardCnHmThird" 340 60 145 90 300 "Connectors" "Third-party Connectors" "Third-party")
            (New-CardVisual "cardCnHmCustomRate" 500 60 145 90 400 "Connectors" "Custom Connector Rate" "Custom %")
            (New-CardVisual "cardCnHmPerEnv" 660 60 145 90 500 "Connectors" "Connectors per Env" "Per Env")
            (New-CardVisual "cardCnHmHighRisk" 820 60 145 90 600 "Connectors" "High-Risk Connectors" "High-Risk")
            (New-CardVisual "cardCnHmNoDlp" 980 60 145 90 700 "Connectors" "Connectors Without DLP Rule" "No DLP Rule")
            (New-CardVisual "cardCnHmReach" 1140 60 100 90 800 "FlowConnectionRefs" "Connector Reach (Flows)" "Reach (Flows)")
            # Heatmap matrix: connector × env (counts)
            ,(New-MatrixVisual "matrixCnHm" 20 165 1220 290 1000 `
                @(@{Table="Connectors"; Column="DisplayName"}) `
                @(@{Table="FlowConnectionRefs"; Measure="Total Flow Connections"}) `
                "Connector Usage by Environment" `
                @(@{Table="Environments"; Column="DisplayName"}))
            # Charts
            (New-DonutVisual "donutCnPub" 20 470 400 240 2000 "Connectors" "PublisherCategory" "Total Connectors" "Publisher Mix")
            (New-DonutVisual "donutCnRisk" 440 470 400 240 2100 "Connectors" "RiskCategory" "Total Connectors" "Risk Mix")
            (New-BarChartVisual "barCnTopUsed" 860 470 380 240 2200 "Connectors" "DisplayName" "Total Flow Connections" "Top-used Connectors")
        )
    }
    # --- NEW PAGE: Maker Pareto ---
    pareto = @{
        displayName = "Maker Pareto"
        visuals = @(
            (New-TextBoxVisual "txtParetoHdr" 20 10 1220 30 10 "Maker Pareto / Concentration Analysis" "18" "#252423" $true)
            (New-CardVisual "cardParAppMakers" 20 60 145 90 100 "Apps" "Unique App Owners" "Unique Makers (Apps)")
            (New-CardVisual "cardParFlowMakers" 180 60 145 90 200 "Flows" "Unique Flow Creators" "Unique Makers (Flows)")
            (New-CardVisual "cardParAvgApps" 340 60 145 90 300 "Apps" "Avg Apps Per Owner" "Avg Apps / Owner")
            (New-CardVisual "cardParAvgFlows" 500 60 145 90 400 "Flows" "Avg Flows Per Creator" "Avg Flows / Owner")
            # Pareto chart — bar of top makers + cumulative line
            (New-BarChartVisual "barParTopAppMakers" 20 165 600 280 1000 "Apps" "OwnerDisplayName" "Total Apps" "Top App Owners")
            (New-BarChartVisual "barParTopFlowMakers" 640 165 600 280 1100 "Flows" "CreatorDisplayName" "Total Flows" "Top Flow Creators")
            # Concentration matrix
            ,(New-MatrixVisual "matrixParMakers" 20 460 1220 250 2000 `
                @(@{Table="Apps"; Column="OwnerDisplayName"}) `
                @(@{Table="Apps"; Measure="Total Apps"},
                  @{Table="Apps"; Measure="Premium API Apps"},
                  @{Table="Apps"; Measure="Stale Apps (90d)"},
                  @{Table="Apps"; Measure="High Risk Apps"},
                  @{Table="Flows"; Measure="Total Flows"},
                  @{Table="Flows"; Measure="Suspended Flows"}) `
                "Maker Activity Matrix")
        )
    }
    # --- NEW PAGE: Agent Maturity Funnel ---
    "agent-funnel" = @{
        displayName = "Agent Maturity Funnel"
        visuals = @(
            (New-TextBoxVisual "txtFunnHdr" 20 10 1220 30 10 "Copilot Agent Adoption Funnel" "18" "#252423" $true)
            (New-CardVisual "cardFunnTotal" 20 60 145 90 100 "CopilotAgents" "Stage 1 - Created Agents" "Created")
            (New-CardVisual "cardFunnConfig" 180 60 145 90 200 "CopilotAgents" "Stage 2 - Configured Agents" "Configured")
            (New-CardVisual "cardFunnPub" 340 60 145 90 300 "CopilotAgents" "Stage 3 - Published Agents" "Published")
            (New-CardVisual "cardFunnAct" 500 60 145 90 400 "CopilotAgents" "Stage 4 - Active 30d" "Active 30d")
            (New-CardVisual "cardFunnEmpty" 660 60 145 90 500 "CopilotAgents" "Empty Agents" "Empty")
            (New-CardVisual "cardFunnAuthRisk" 820 60 145 90 600 "CopilotAgents" "Auth Risk Agents" "Auth Risk")
            (New-CardVisual "cardFunnKnowCov" 980 60 145 90 700 "CopilotAgents" "Knowledge Coverage" "Knowledge Cov.")
            (New-CardVisual "cardFunnCfgRate" 1140 60 100 90 800 "CopilotAgents" "Funnel: Configured Rate" "Cfg Rate")
            # Stacked column or bar funnel approximation (using built-in funnel)
            (New-MultiKpiCardVisual "kpiFunnStages" 20 165 1220 110 1000 @(
                @{Table="CopilotAgents"; Measure="Stage 1 - Created Agents"},
                @{Table="CopilotAgents"; Measure="Stage 2 - Configured Agents"},
                @{Table="CopilotAgents"; Measure="Stage 3 - Published Agents"},
                @{Table="CopilotAgents"; Measure="Stage 4 - Active 30d"}
            ) "Funnel Stages")
            # Authentication & component charts
            (New-DonutVisual "donutFunnAuth" 20 290 400 240 2000 "CopilotAgents" "AuthenticationMode" "Total Agents" "Authentication Modes")
            (New-DonutVisual "donutFunnComp" 440 290 400 240 2100 "CopilotComponents" "ComponentType" "Total Components" "Component Mix")
            (New-BarChartVisual "barFunnTopMakers" 860 290 380 240 2200 "CopilotAgents" "CreatedByName" "Total Agents" "Top Agent Creators")
            # Detail
            (New-TableVisual "tblFunnAgents" 20 545 1220 165 3000 "CopilotAgents" @("DisplayName","StatusReason","TopicCount","KnowledgeSourceCount","SkillCount","AuthenticationMode","CreatedByName","ModifiedOn","EnvironmentName") "Agent Details")
        )
    }
    # --- NEW PAGE: Premium Licensing Exposure ---
    premium = @{
        displayName = "Premium Licensing"
        visuals = @(
            (New-TextBoxVisual "txtPremHdr" 20 10 1220 30 10 "Premium API &amp; Licensing Exposure" "18" "#252423" $true)
            (New-CardVisual "cardPremApps" 20 60 145 90 100 "Apps" "Premium API Apps" "Premium Apps")
            (New-CardVisual "cardPremRate" 180 60 145 90 200 "Apps" "Premium Exposure Rate" "Exposure %")
            (New-CardVisual "cardPremMakers" 340 60 145 90 300 "Apps" "Premium Maker Count" "Premium Makers")
            (New-CardVisual "cardPremCustomApi" 500 60 145 90 400 "Apps" "Custom API Apps" "Custom API Apps")
            (New-CardVisual "cardPremConn" 660 60 145 90 500 "Connectors" "Premium Connectors" "Premium Connectors")
            (New-CardVisual "cardPremConnRate" 820 60 145 90 600 "Connectors" "Premium Connector Rate" "Premium %")
            # Charts
            (New-BarChartVisual "barPremByEnv" 20 165 600 270 1000 "Environments" "DisplayName" "Premium API Apps" "Premium Apps by Environment")
            (New-BarChartVisual "barPremByMaker" 640 165 600 270 1100 "Apps" "OwnerDisplayName" "Premium API Apps" "Premium Apps by Owner")
            # Premium connectors leaderboard
            (New-TableVisual "tblPremCon" 20 450 600 260 2000 "Connectors" @("DisplayName","Tier","Publisher","PublisherCategory","RiskCategory","EnvironmentName") "Premium Connectors")
            (New-TableVisual "tblPremApps" 640 450 600 260 2100 "Apps" @("DisplayName","OwnerDisplayName","UsesPremiumApi","UsesCustomApi","SharedUsersCount","EnvironmentName") "Premium Apps")
        )
    }
    # --- NEW PAGE: Capacity & Sprawl ---
    capacity = @{
        displayName = "Capacity & Sprawl"
        visuals = @(
            (New-TextBoxVisual "txtCapHdr" 20 10 1220 30 10 "Capacity Utilization &amp; Environment Sprawl" "18" "#252423" $true)
            (New-CardVisual "cardCapTotal" 20 60 145 90 100 "Environments" "Total Capacity GB" "Total GB")
            (New-CardVisual "cardCapAvg" 180 60 145 90 200 "Environments" "Avg Capacity GB Per Env" "Avg GB / Env")
            (New-CardVisual "cardCapHeavy" 340 60 145 90 300 "Environments" "Heavy Capacity Envs (>50GB)" "Heavy Envs")
            (New-CardVisual "cardCapTop" 500 60 145 90 400 "Environments" "Top Capacity Env" "Top Env GB")
            (New-CardVisual "cardCapEmpty" 660 60 145 90 500 "Environments" "Empty Environments" "Empty Envs")
            (New-CardVisual "cardCapEmptyRate" 820 60 145 90 600 "Environments" "Empty Env Rate" "Empty %")
            (New-CardVisual "cardCapRegions" 980 60 145 90 700 "Environments" "Region Count" "Regions")
            (New-CardVisual "cardCapSprawl" 1140 60 100 90 800 "Environments" "Sprawl Index" "Sprawl Idx")
            # Charts
            (New-StackedColumnChartVisual "stkCapByEnv" 20 165 600 270 1000 "Environments" "DisplayName" "Environments" "EnvironmentType" "Environments" "Total Database MB" "DB MB by Env")
            (New-BarChartVisual "barCapByRegion" 640 165 600 270 1100 "Environments" "Region" "Total Capacity GB" "Capacity by Region")
            (New-DonutVisual "donutCapBand" 20 450 400 260 2000 "Environments" "CapacityBand" "Total Environments" "Environments by Capacity Band")
            (New-DonutVisual "donutCapLifecycle" 440 450 400 260 2100 "Environments" "EnvLifecycleStage" "Total Environments" "Environment Lifecycle")
            (New-TableVisual "tblCapBig" 860 450 380 260 2200 "Environments" @("DisplayName","CapacityBand","DatabaseUsedMb","FileUsedMb","LogUsedMb","ResourceTotal") "Top Capacity")
        )
    }
    # --- NEW PAGE: What's New (last 30 days) ---
    "whats-new" = @{
        displayName = "What's New (30d)"
        visuals = @(
            (New-TextBoxVisual "txtWnHdr" 20 10 1220 30 10 "What's New · Last 30 Days" "18" "#252423" $true)
            (New-CardVisual "cardWnApps" 20 60 145 90 100 "Apps" "Apps Created Last 30d Calendar" "New Apps")
            (New-CardVisual "cardWnFlows" 180 60 145 90 200 "Flows" "Flows Created Last 30d Calendar" "New Flows")
            (New-CardVisual "cardWnAgents" 340 60 145 90 300 "CopilotAgents" "Agents Created Last 30d" "New Agents")
            (New-CardVisual "cardWnConn" 500 60 145 90 400 "Connections" "Connections Created Last 30d" "New Connections")
            (New-CardVisual "cardWnAppsMod" 660 60 145 90 500 "Apps" "Apps Modified Last 30d" "Apps Touched")
            (New-CardVisual "cardWnFlowsMod" 820 60 145 90 600 "Flows" "Flows Modified Last 30d" "Flows Touched")
            (New-CardVisual "cardWnVel" 980 60 145 90 700 "Flows" "Daily Resource Velocity" "Daily Velocity")
            (New-CardVisual "cardWnNet" 1140 60 100 90 800 "Flows" "Net New Resources 30d" "Net New")
            # Detail tables — items created in last 30 days (uses inactive rel, falls back to slicer Calendar IsLast30Days)
            (New-LineChartVisual "lineWnCreate" 20 165 1220 200 1000 "Calendar" "Date" "Apps" "Total Apps" "Daily Apps Created")
            (New-TableVisual "tblWnApps" 20 380 600 330 2000 "Apps" @("DisplayName","OwnerDisplayName","CreatedTime","AppType","EnvironmentName") "New Apps")
            (New-TableVisual "tblWnFlows" 640 380 600 330 2100 "Flows" @("DisplayName","CreatorDisplayName","CreatedTime","TriggerType","EnvironmentName") "New Flows")
        )
    }
    # --- NEW PAGE: Data Health ---
    "data-health" = @{
        displayName = "Data Health"
        visuals = @(
            (New-TextBoxVisual "txtDhHdr" 20 10 1220 30 10 "Collection Telemetry / Data Pipeline Health" "18" "#252423" $true)
            (New-CardVisual "cardDhTotal" 20 60 145 90 100 "Errors" "Total Errors" "Total Errors")
            (New-CardVisual "cardDhAuth" 180 60 145 90 200 "Errors" "Auth Errors" "Auth")
            (New-CardVisual "cardDhThr" 340 60 145 90 300 "Errors" "Throttle Errors" "Throttle (429)")
            (New-CardVisual "cardDhTo" 500 60 145 90 400 "Errors" "Timeout Errors" "Timeout")
            (New-CardVisual "cardDhSrv" 660 60 145 90 500 "Errors" "Server Errors" "Server (5xx)")
            (New-CardVisual "cardDhAffEnv" 820 60 145 90 600 "Errors" "Affected Environments" "Affected Envs")
            (New-CardVisual "cardDhLastCol" 980 60 145 90 700 "Environments" "Last Collected" "Last Collected")
            (New-CardVisual "cardDhDays" 1140 60 100 90 800 "Environments" "Days Since Collection" "Days Old")
            # Charts
            (New-BarChartVisual "barDhPhase" 20 165 600 270 1000 "Errors" "Phase" "Total Errors" "Errors by Collection Phase")
            (New-DonutVisual "donutDhType" 640 165 600 270 1100 "Errors" "ErrorType" "Total Errors" "Error Type Distribution")
            (New-LineChartVisual "lineDhTime" 20 450 600 260 2000 "Errors" "Timestamp" "Errors" "Total Errors" "Errors over Time")
            (New-TableVisual "tblDhDetails" 640 450 600 260 2100 "Errors" @("EnvironmentName","Phase","ErrorType","Error","Timestamp") "Recent Errors")
        )
    }
    # --- NEW PAGE: Glossary ---
    glossary = @{
        displayName = "Glossary"
        visuals = @(
            (New-TextBoxVisual "txtGlHdr" 20 10 1220 30 10 "KPI Glossary &amp; Definitions" "20" "#252423" $true)
            (New-TextBoxVisual "txtGlIntro" 20 50 1220 60 20 "<b>This dashboard contains 180+ DAX measures.</b> Below are the most important governance KPIs and their definitions. All measures are also visible in the data pane grouped by <i>display folder</i>." "13")
            (New-TextBoxVisual "txtGlGov" 20 120 600 30 50 "Governance &amp; Risk" "16" "#0078D4" $true)
            (New-TextBoxVisual "txtGlGov1" 20 155 600 200 60 "<b>Governance Score v2</b> — weighted 0-100 score across 10 dimensions: security (15), DLP (15), flow health (10), solutions (15), lifecycle (10), sharing (10), HTTP risk (10), orphans (5), premium control (5), agent auth (5).<br/><br/><b>Shadow IT Rate</b> — % of resources unmanaged or not in solutions.<br/><br/><b>HTTP Risk Score</b> — weighted: HTTP raw × 3 + Webhook × 2 + Entra × 1.<br/><br/><b>Sprawl Index</b> — environments per region.<br/><br/><b>Risk Score (App / Flow)</b> — sum: stale + orphaned + over-shared + non-solution + bypass-consent." "12")
            (New-TextBoxVisual "txtGlTime" 640 120 600 30 70 "Time Intelligence" "16" "#0078D4" $true)
            (New-TextBoxVisual "txtGlTime1" 640 155 600 200 80 "<b>Apps / Flows Created MTD/YTD</b> — month-/year-to-date counts via Calendar relationship.<br/><br/><b>Apps YoY %</b> — (this YTD - last YTD) / last YTD.<br/><br/><b>Net New Resources 30d</b> — Apps + Flows created in last 30 days.<br/><br/><b>Daily Resource Velocity</b> — Net New / 30.<br/><br/><b>Modified Last 30d</b> — uses inactive Calendar→ModifiedDate relationship via USERELATIONSHIP." "12")
            (New-TextBoxVisual "txtGlMaker" 20 380 600 30 90 "Makers &amp; Adoption" "16" "#0078D4" $true)
            (New-TextBoxVisual "txtGlMaker1" 20 415 600 200 100 "<b>Unique App Owners</b> — DISTINCTCOUNT of OwnerObjectId.<br/><br/><b>Avg Apps Per Owner</b> — Total Apps / Unique Owners.<br/><br/><b>Maker App Rank</b> — RANKX over all owners by Total Apps.<br/><br/><b>Funnel Configured / Publish / Activation Rates</b> — agent maturity funnel transition rates." "12")
            (New-TextBoxVisual "txtGlConn" 640 380 600 30 110 "Connectors &amp; Endpoints" "16" "#0078D4" $true)
            (New-TextBoxVisual "txtGlConn1" 640 415 600 200 120 "<b>Connector Utilization</b> — distinct connectors used in flows / total registered connectors.<br/><br/><b>Custom Connector Rate</b> — non-Microsoft connectors %.<br/><br/><b>Connectors Without DLP Rule</b> — connectors not classified by any policy (gap).<br/><br/><b>Flow External Action Endpoints</b> — actions whose host is not microsoft / sharepoint / azure / dynamics." "12")
        )
    }
    # --- NEW DRILLTHROUGH PAGE: Maker Detail ---
    "maker-details" = @{
        displayName = "Maker Detail"
        visuals = @(
            (New-SlicerVisual "slicerMakerDetail" 20 20 200 80 50 "Apps" "OwnerDisplayName" "Select Maker")
            (New-CardVisual "cardMkdApps" 240 20 145 80 100 "Apps" "Total Apps" "Apps Owned")
            (New-CardVisual "cardMkdFlows" 400 20 145 80 200 "Flows" "Total Flows" "Flows Created")
            (New-CardVisual "cardMkdPrem" 560 20 145 80 300 "Apps" "Premium API Apps" "Premium")
            (New-CardVisual "cardMkdStale" 720 20 145 80 400 "Apps" "Stale Apps (90d)" "Stale")
            (New-CardVisual "cardMkdRisk" 880 20 145 80 500 "Apps" "Avg App Risk" "Avg Risk")
            (New-CardVisual "cardMkdShared" 1040 20 200 80 600 "Apps" "Total Shared Users" "Shared Users")
            # Donut + bar
            (New-DonutVisual "donutMkdType" 20 120 400 250 1000 "Apps" "AppType" "Total Apps" "App Types")
            (New-DonutVisual "donutMkdSharing" 440 120 400 250 1100 "Apps" "SharingRiskBand" "Total Apps" "Sharing Bands")
            (New-DonutVisual "donutMkdLifecycle" 860 120 380 250 1200 "Apps" "LifecyclePhase" "Total Apps" "Lifecycle")
            # Tables
            (New-TableVisual "tblMkdApps" 20 390 1220 320 2000 "Apps" @("DisplayName","AppType","SharingRiskBand","RiskBand","StalenessStatus","UsesPremiumApi","SharedUsersCount","LastModifiedTime","EnvironmentName") "All Apps Owned by Maker")
        )
    }
}

# --- Drillthrough configuration: detail pages get a Drillthrough filter ---
# Right-click any visual showing the matching column → "Drill through" → page name.
# The drillthrough field is shown but the page remains menu-accessible too.
$drillthroughConfig = @{
    "env-details"       = @{ Table = "Environments"; Column = "DisplayName" }
    "app-details"       = @{ Table = "Apps"; Column = "DisplayName" }
    "flow-details"      = @{ Table = "Flows"; Column = "DisplayName" }
    "agent-details"     = @{ Table = "CopilotAgents"; Column = "DisplayName" }
    "dlp-details"       = @{ Table = "DlpPolicies"; Column = "DisplayName" }
    "connector-details" = @{ Table = "Connectors"; Column = "DisplayName" }
    "maker-details"     = @{ Table = "Apps"; Column = "OwnerDisplayName" }
}

# --- Generate page files ---

foreach ($pageName in $pageNames) {
    $pageDef = $pageDefs[$pageName]
    $pageDir = "$defDir/pages/$pageName"

    $pageJson = [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.3.0/schema.json"
        name = $pageName
        displayName = $pageDef.displayName
        displayOption = "FitToPage"
        height = 720
        width = 1280
    }

    # Drillthrough configuration — detail pages accept a column-bound filter
    if ($drillthroughConfig.ContainsKey($pageName)) {
        $dt = $drillthroughConfig[$pageName]
        $pageJson.filters = @(
            [ordered]@{
                name = "drillthrough_$($dt.Column)"
                field = (New-ColField $dt.Table $dt.Column)
                type = "Categorical"
                howCreated = "Drilled"
            }
        )
        $pageJson.pageBinding = [ordered]@{
            type = "Drillthrough"
            name = $pageName
        }
    }

    Write-JsonFile "$pageDir/page.json" $pageJson

    foreach ($visual in $pageDef.visuals) {
        $visualName = $visual.name
        Write-JsonFile "$pageDir/visuals/$visualName/visual.json" $visual
    }
}

# ============================================================================
# DONE
# ============================================================================

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Enterprise Governance PBIP v4 created — 28 pages!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
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
Write-Host "28 Pages:" -ForegroundColor Yellow
Write-Host "  Tier 1 - Command Center:" -ForegroundColor Cyan
Write-Host "    1. Executive Command Center" -ForegroundColor Gray
Write-Host "  Tier 2 - Domain Governance:" -ForegroundColor Cyan
Write-Host "    2. Environment Health" -ForegroundColor Gray
Write-Host "    3. App Portfolio" -ForegroundColor Gray
Write-Host "    4. Flow Operations" -ForegroundColor Gray
Write-Host "    5. Copilot Agents" -ForegroundColor Gray
Write-Host "    6. Connector & DLP Governance" -ForegroundColor Gray
Write-Host "    7. Endpoint & API Security" -ForegroundColor Gray
Write-Host "  Tier 3 - Risk & Intelligence (incl. NEW pages):" -ForegroundColor Cyan
Write-Host "    8. Risk & Shadow IT" -ForegroundColor Gray
Write-Host "    9. Maker Activity & Ownership" -ForegroundColor Gray
Write-Host "   10. Connection Intelligence" -ForegroundColor Gray
Write-Host "   11. Trends & Velocity                  (NEW)" -ForegroundColor Green
Write-Host "   12. Compliance Heatmap                 (NEW)" -ForegroundColor Green
Write-Host "   13. Sharing & Permissions              (NEW · uses AppPermissions/FlowPermissions)" -ForegroundColor Green
Write-Host "   14. HTTP Endpoint Risk                 (NEW)" -ForegroundColor Green
Write-Host "   15. Connector Heatmap                  (NEW)" -ForegroundColor Green
Write-Host "   16. Maker Pareto                       (NEW)" -ForegroundColor Green
Write-Host "   17. Agent Maturity Funnel              (NEW)" -ForegroundColor Green
Write-Host "   18. Premium Licensing Exposure         (NEW)" -ForegroundColor Green
Write-Host "   19. Capacity & Sprawl                  (NEW)" -ForegroundColor Green
Write-Host "   20. What's New (30d)                   (NEW)" -ForegroundColor Green
Write-Host "   21. Data Health (Errors)               (NEW · uses Errors.csv)" -ForegroundColor Green
Write-Host "   22. Glossary                           (NEW · KPI definitions)" -ForegroundColor Green
Write-Host "  Tier 4 - Detail / Drillthrough (right-click any matching column):" -ForegroundColor Cyan
Write-Host "   23. Environment Detail" -ForegroundColor Gray
Write-Host "   24. App Detail" -ForegroundColor Gray
Write-Host "   25. Flow Detail" -ForegroundColor Gray
Write-Host "   26. Agent Detail" -ForegroundColor Gray
Write-Host "   27. DLP Policy Detail" -ForegroundColor Gray
Write-Host "   28. Connector Detail" -ForegroundColor Gray
Write-Host "   29. Maker Detail                       (NEW · drillthrough on owner)" -ForegroundColor Green
Write-Host ""
Write-Host "Tables (18 total — 4 NEW):" -ForegroundColor Yellow
Write-Host "  Existing: Environments, Apps, Flows, Connectors, DlpPolicies," -ForegroundColor Gray
Write-Host "  DlpConnectorRules, UsageAnalytics, AppConnectorRefs, FlowActions," -ForegroundColor Gray
Write-Host "  FlowTriggers, FlowConnectionRefs, CopilotAgents, CopilotComponents, Connections" -ForegroundColor Gray
Write-Host "  NEW: Calendar (date dim), AppPermissions, FlowPermissions, Errors" -ForegroundColor Green
Write-Host ""
Write-Host "Relationships (28+ — incl. inactive Calendar relationships for time intelligence):" -ForegroundColor Yellow
Write-Host "  Calendar -> Apps[CreatedDate] | Flows[CreatedDate] | UsageAnalytics[DateOnly]" -ForegroundColor Gray
Write-Host "  Calendar -> Apps[ModifiedDate] (inactive)  | Flows[ModifiedDate] (inactive)" -ForegroundColor Gray
Write-Host "  AppPermissions -> Apps  | FlowPermissions -> Environments" -ForegroundColor Gray
Write-Host "  Errors -> Environments  | Connectors -> AppConnectorRefs/FlowActions/Connections (via ConnectorKey)" -ForegroundColor Gray
Write-Host ""
Write-Host "Calculated columns added (for slicing):" -ForegroundColor Yellow
Write-Host "  Apps:  CreatedDate, ModifiedDate, PublishedDate, AppAgeDays, DaysSinceModified," -ForegroundColor Gray
Write-Host "         LifecyclePhase, SharingRiskBand, IsOrphaned, RiskScore, RiskBand" -ForegroundColor Gray
Write-Host "  Flows: CreatedDate, ModifiedDate, FlowAgeDays, LifecyclePhase, TriggerCategory," -ForegroundColor Gray
Write-Host "         ActionCount, ComplexityBand, IsOrphaned, RiskScore, RiskBand" -ForegroundColor Gray
Write-Host "  Environments: TotalCapacityMb, CapacityBand, ResourceTotal, IsEmpty, IsSecured," -ForegroundColor Gray
Write-Host "                EnvDensity, EnvAgeDays, EnvLifecycleStage" -ForegroundColor Gray
Write-Host "  Connectors: ConnectorKey, RiskCategory, PublisherCategory" -ForegroundColor Gray
Write-Host "  FlowActions/Triggers/AppConnRefs: ConnectorKey, BaseDomain, IsExternalHost" -ForegroundColor Gray
Write-Host "  Connections: ConnectorRefKey, CreatedDate, ConnectionAgeDays, AgeBand, IsHealthy" -ForegroundColor Gray
Write-Host "  Permissions: PrincipalCategory, AccessLevel" -ForegroundColor Gray
Write-Host "  Errors: ErrorType" -ForegroundColor Gray
Write-Host ""
Write-Host "180+ DAX measures including:" -ForegroundColor Yellow
Write-Host "  Governance: Governance Score, Governance Score v2 (10-dim weighted), Shadow IT Rate" -ForegroundColor Gray
Write-Host "  Time intel: Apps/Flows MTD/QTD/YTD, YoY %, Last 7/30/90d, Net New 30d, Velocity" -ForegroundColor Gray
Write-Host "  Risk:       App/Flow/HTTP Risk Scores, High/Medium Risk counts, Orphan rates" -ForegroundColor Gray
Write-Host "  Sharing:    Tenant-wide / Org-shared apps, Avg Sharing Breadth, Co-Owner counts" -ForegroundColor Gray
Write-Host "  Capacity:   Total/Avg GB, Heavy Envs, Empty Envs, Sprawl Index, DB:File Ratio" -ForegroundColor Gray
Write-Host "  Connector:  Custom Rate, Premium Rate, Connectors Without DLP Rule, Connector Reach" -ForegroundColor Gray
Write-Host "  Agent:      Maturity Funnel (4 stages + transition rates), Auth Risk, Knowledge Coverage" -ForegroundColor Gray
Write-Host "  Endpoint:   HTTP Risk Score, External vs Internal, Distinct Domains" -ForegroundColor Gray
Write-Host "  Premium:    Premium Maker Count, Exposure Rate" -ForegroundColor Gray
Write-Host "  Data Health: Auth/Throttle/Timeout/Server error counts, Last Collected" -ForegroundColor Gray
Write-Host ""
Write-Host "Polish:" -ForegroundColor Yellow
Write-Host "  - Custom theme: PowerPlatformGovernance.json (semantic risk colors, Segoe UI)" -ForegroundColor Gray
Write-Host "  - Drillthrough: 7 detail pages — right-click any matching column" -ForegroundColor Gray
Write-Host "  - Calendar relationships enable proper time intelligence" -ForegroundColor Gray
Write-Host ""
