<#
.SYNOPSIS
    Generates an Enterprise Governance Power BI Dashboard (PBIP) from Power Platform CSV exports.
.DESCRIPTION
    Creates a 12-page governance-focused PBIP project with:
    - ~60 DAX measures (risk scores, staleness, trends, ratios)
    - Visual types: card, donut, bar, lineChart, treemap, gauge, slicer, columnChart, matrix, table
    - Environment slicer filtering on key pages
    - KPI banner → insights row → detail table layout per page

    Pages: Executive Summary, Environment Governance, App Inventory & Risk,
           Flow Health & Operations, Connector Risk Analysis, DLP & Compliance,
           Endpoint & API Risk, Shadow IT & Lifecycle

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
    param([string]$TableName, [hashtable[]]$TypeMappings)
    $typeLines = ($TypeMappings | ForEach-Object {
        "        {`"$($_.Name)`", $($_.Type)}"
    }) -join ",`n"

    $mExpr = @(
        "let"
        "    Source = Csv.Document(File.Contents(CsvFolderPath & `"$TableName.csv`"), [Delimiter=`",`", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),"
        "    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),"
        "    Typed = Table.TransformColumnTypes(Headers, {"
        $typeLines
        "    })"
        "in"
        "    Typed"
    )

    [ordered]@{
        name = "$TableName-Partition"
        mode = "import"
        source = [ordered]@{ type = "m"; expression = $mExpr }
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
Get-ChildItem -Path $OutputPath -Directory -Filter "*.Report" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
Get-ChildItem -Path $OutputPath -Directory -Filter "*.SemanticModel" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
Get-ChildItem -Path $OutputPath -Directory -Filter ".pbi" -Recurse -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force

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
        (New-MeasureDef "Governance Score" "[Env Security Rate] * 0.25 + (1 - [Suspension Rate]) * 0.25 + (1 - [Stale App Rate]) * 0.25 + [DLP Coverage Rate] * 0.25" "0.0%" "Governance")
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
    )
}

$tFlows = [ordered]@{
    name = "Flows"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId" "string" "none" -IsKey $true)
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
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="EnvironmentName"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="Description"; Type="type text"}, @{Name="State"; Type="type text"},
        @{Name="CreatorObjectId"; Type="type text"}, @{Name="CreatorDisplayName"; Type="type text"},
        @{Name="CreatedTime"; Type="type datetime"}, @{Name="LastModifiedTime"; Type="type datetime"},
        @{Name="TriggerType"; Type="type text"}, @{Name="IsSolutionAware"; Type="type logical"},
        @{Name="SolutionId"; Type="type text"}, @{Name="IsManaged"; Type="type logical"},
        @{Name="SuspensionReason"; Type="type text"}, @{Name="CollectedAt"; Type="type datetime"}
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
        (New-MeasureDef "Connector Utilization" "DIVIDE(DISTINCTCOUNT('AppConnectorRefs'[ConnectorId]), COUNTROWS('Connectors'), 0)" "0.0%")
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
        (New-MeasureDef "DLP Coverage Rate" "DIVIDE([Enabled Policies], CALCULATE(COUNTROWS('Environments'), ALL('Environments')), 0)" "0.0%" "Governance")
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
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="ActionType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}
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
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="TriggerType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"},
        @{Name="BaseUrl"; Type="type text"}
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
        (New-ColumnDef "EnvironmentId")
        (New-ColumnDef "ConnectorId")
        (New-ColumnDef "ConnectionName")
        (New-ColumnDef "ConnectionUrl")
    )
    partitions = @((New-CsvPartition "FlowConnectionRefs" @(
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="ConnectionName"; Type="type text"},
        @{Name="ConnectionUrl"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Flow Connections" "COUNTROWS('FlowConnectionRefs')")
        (New-MeasureDef "Distinct Connection URLs" "DISTINCTCOUNT('FlowConnectionRefs'[ConnectionUrl])")
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
                    $tDlpPolicies, $tDlpRules, $tUsage, $tAppConnRefs, $tFlowActions, $tFlowTriggers, $tFlowConnRefs)
        relationships = @(
            (New-RelationshipDef "rel_Apps_Env" "Apps" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Flows_Env" "Flows" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connectors_Env" "Connectors" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_DlpRules_Policy" "DlpConnectorRules" "PolicyId" "DlpPolicies" "PolicyId")
            (New-RelationshipDef "rel_Usage_Env" "UsageAnalytics" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_AppConnRefs_Apps" "AppConnectorRefs" "AppId" "Apps" "AppId")
            (New-RelationshipDef "rel_FlowActions_Flows" "FlowActions" "FlowId" "Flows" "FlowId")
            (New-RelationshipDef "rel_FlowTriggers_Flows" "FlowTriggers" "FlowId" "Flows" "FlowId")
            (New-RelationshipDef "rel_FlowConnRefs_Flows" "FlowConnectionRefs" "FlowId" "Flows" "FlowId")
        )
        expressions = @(
            [ordered]@{
                name = "CsvFolderPath"
                kind = "m"
                expression = @("`"$($CsvPath -replace '\\', '\\')`" meta [IsParameterQuery=true, Type=`"Text`", IsParameterQueryRequired=true]")
            }
        )
        annotations = @(
            @{ name = "PBI_QueryOrder"; value = "[`"Environments`",`"Apps`",`"Flows`",`"Connectors`",`"DlpPolicies`",`"DlpConnectorRules`",`"UsageAnalytics`",`"AppConnectorRefs`",`"FlowActions`",`"FlowTriggers`",`"FlowConnectionRefs`"]" }
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
# 12 PAGES (8 governance + 4 detail)
# ============================================================================

$pageNames = @("executive", "environments", "apps", "flows", "connectors", "dlp", "endpoints", "lifecycle", "app-details", "flow-details", "env-details", "dlp-details")
Write-JsonFile "$defDir/pages/pages.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json"
    pageOrder = $pageNames
    activePageName = "executive"
})

# --- Page 1: Executive Summary ---
$execMatrix = New-MatrixVisual "matrixMetrics" 20 480 875 230 8000 `
    @(@{Table="Environments"; Column="DisplayName"}) `
    @(@{Table="Apps"; Measure="Total Apps"},
      @{Table="Flows"; Measure="Total Flows"},
      @{Table="Connectors"; Measure="Total Connectors"},
      @{Table="Flows"; Measure="Suspended Flows"}) `
    "Key Metrics by Environment"

$pageDefs = @{
    executive = @{
        displayName = "Executive Summary"
        visuals = @(
            (New-SlicerVisual "slicerEnvExec" 20 10 175 55 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardExecEnv" 210 10 145 55 100 "Environments" "Total Environments" "Environments")
            (New-CardVisual "cardExecApps" 375 10 145 55 200 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardExecFlows" 540 10 145 55 300 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardExecConn" 705 10 145 55 400 "Connectors" "Total Connectors" "Connectors")
            (New-GaugeVisual "gaugeGovScore" 20 80 280 185 1000 "Environments" "Governance Score" "Governance Score")
            (New-GaugeVisual "gaugeSuspRate" 315 80 280 185 2000 "Flows" "Suspension Rate" "Suspension Rate")
            (New-GaugeVisual "gaugeStaleRate" 610 80 280 185 3000 "Apps" "Stale App Rate" "Stale App Rate")
            (New-TreemapVisual "tmEnvTypes" 20 275 430 195 5000 "Environments" "EnvironmentType" "Apps" "Total Apps" "Environments by Type")
            (New-ColumnChartVisual "colGrowth" 465 275 430 195 6000 "Environments" "DisplayName" "Apps" "Apps Created Last 30d" "Resource Growth (30d)")
            $execMatrix
        )
    }
    environments = @{
        displayName = "Environment Governance"
        visuals = @(
            (New-CardVisual "cardEnvTotal" 20 20 145 80 100 "Environments" "Total Environments" "Environments")
            (New-CardVisual "cardEnvProd" 185 20 145 80 200 "Environments" "Production Environments" "Production")
            (New-CardVisual "cardEnvSandbox" 350 20 145 80 300 "Environments" "Sandbox Environments" "Sandbox")
            (New-CardVisual "cardEnvUnsecured" 515 20 145 80 400 "Environments" "Unsecured Environments" "Unsecured")
            (New-CardVisual "cardEnvSecRate" 680 20 145 80 500 "Environments" "Env Security Rate" "Security Rate")
            (New-DonutVisual "donutEnvType2" 20 120 430 260 1000 "Environments" "EnvironmentType" "Total Environments" "Environment Types")
            (New-BarChartVisual "barCapacity2" 470 120 430 260 2000 "Environments" "DisplayName" "Total Capacity GB" "Capacity by Environment")
            (New-TableVisual "tblEnv2" 20 400 880 300 3000 "Environments" @("DisplayName","EnvironmentType","Region","IsDataverseEnabled","DatabaseUsedMb","FileUsedMb","SecurityGroupId") "Environment Details")
        )
    }
    apps = @{
        displayName = "App Inventory & Risk"
        visuals = @(
            (New-SlicerVisual "slicerEnvApps" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardAppTotal" 210 20 120 80 100 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardAppStale" 345 20 120 80 200 "Apps" "Stale Apps (90d)" "Stale")
            (New-CardVisual "cardAppOrphan" 480 20 120 80 300 "Apps" "Orphaned Apps" "Orphaned")
            (New-CardVisual "cardAppWide" 615 20 120 80 400 "Apps" "Widely Shared Apps" "Widely Shared")
            (New-CardVisual "cardAppBypass" 750 20 120 80 500 "Apps" "Bypass Consent Apps" "Bypass Consent")
            (New-DonutVisual "donutAppType2" 20 120 430 240 1000 "Apps" "AppType" "Total Apps" "App Types")
            (New-BarChartVisual "barAppEnv2" 470 120 430 240 2000 "Apps" "EnvironmentName" "Total Apps" "Apps by Environment")
            (New-ColumnChartVisual "colAppGrowth" 20 380 430 320 3000 "Apps" "CreatedTime" "Apps" "Total Apps" "App Creation Trend")
            (New-TableVisual "tblApps2" 470 380 430 320 4000 "Apps" @("DisplayName","AppType","OwnerDisplayName","EnvironmentName","SharedUsersCount","LastModifiedTime") "App Details")
        )
    }
    flows = @{
        displayName = "Flow Health & Operations"
        visuals = @(
            (New-SlicerVisual "slicerEnvFlows" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardFlowTotal" 210 20 120 80 100 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardFlowActive" 345 20 120 80 200 "Flows" "Active Flows" "Active")
            (New-CardVisual "cardFlowSusp" 480 20 120 80 300 "Flows" "Suspended Flows" "Suspended")
            (New-CardVisual "cardFlowSuspRate" 615 20 120 80 400 "Flows" "Suspension Rate" "Suspension Rate")
            (New-CardVisual "cardFlowStale" 750 20 120 80 500 "Flows" "Stale Flows (90d)" "Stale")
            (New-DonutVisual "donutFlowState2" 20 120 280 240 1000 "Flows" "State" "Total Flows" "Flow States")
            (New-BarChartVisual "barFlowEnv2" 315 120 280 240 2000 "Flows" "EnvironmentName" "Total Flows" "Flows by Environment")
            (New-DonutVisual "donutTrigger" 610 120 280 240 3000 "Flows" "TriggerType" "Total Flows" "Trigger Types")
            (New-BarChartVisual "barSuspReason" 20 380 430 320 4000 "Flows" "SuspensionReason" "Suspended Flows" "Suspension Reasons")
            (New-TableVisual "tblFlows2" 470 380 430 320 5000 "Flows" @("DisplayName","State","CreatorDisplayName","TriggerType","EnvironmentName","SuspensionReason","LastModifiedTime") "Flow Details")
        )
    }
    connectors = @{
        displayName = "Connector Risk Analysis"
        visuals = @(
            (New-SlicerVisual "slicerEnvConn" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardConnTotal" 210 20 135 80 100 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardConnPrem" 360 20 135 80 200 "Connectors" "Premium Connectors" "Premium")
            (New-CardVisual "cardConnCustom" 510 20 135 80 300 "Connectors" "Custom Connectors" "Custom")
            (New-CardVisual "cardConnStd" 660 20 135 80 400 "Connectors" "Standard Connectors" "Standard")
            (New-DonutVisual "donutConnTier2" 20 120 430 240 1000 "Connectors" "Tier" "Total Connectors" "Tier Distribution")
            (New-TreemapVisual "tmTopConn" 470 120 430 240 2000 "Connectors" "DisplayName" "Connectors" "Total Connectors" "Top Connectors")
            (New-BarChartVisual "barConnEnv2" 20 380 430 320 3000 "Connectors" "EnvironmentName" "Total Connectors" "Connectors by Environment")
            (New-TableVisual "tblConn2" 470 380 430 320 4000 "Connectors" @("DisplayName","Tier","Publisher","IsCustom","EnvironmentName") "Connector Details")
        )
    }
    dlp = @{
        displayName = "DLP & Compliance"
        visuals = @(
            (New-CardVisual "cardDlpTotal" 20 20 145 80 100 "DlpPolicies" "Total DLP Policies" "DLP Policies")
            (New-CardVisual "cardDlpEnabled" 185 20 145 80 200 "DlpPolicies" "Enabled Policies" "Enabled")
            (New-CardVisual "cardDlpBiz" 350 20 145 80 300 "DlpConnectorRules" "Business Connectors" "Business")
            (New-CardVisual "cardDlpNonBiz" 515 20 145 80 400 "DlpConnectorRules" "Non-Business Connectors" "Non-Business")
            (New-CardVisual "cardDlpBlocked" 680 20 145 80 500 "DlpConnectorRules" "Blocked Connectors" "Blocked")
            (New-DonutVisual "donutDlpClass2" 20 120 430 240 1000 "DlpConnectorRules" "Classification" "Total Connector Rules" "Classification Breakdown")
            (New-BarChartVisual "barDlpScope" 470 120 430 240 2000 "DlpPolicies" "EnvironmentScope" "Total DLP Policies" "Policy Scope")
            (New-TableVisual "tblDlpPol2" 20 380 880 150 3000 "DlpPolicies" @("DisplayName","IsEnabled","EnvironmentScope","LastModifiedTime") "DLP Policies")
            (New-TableVisual "tblDlpRules2" 20 545 880 165 4000 "DlpConnectorRules" @("PolicyName","ConnectorName","Classification") "Connector Rules")
        )
    }
    endpoints = @{
        displayName = "Endpoint & API Risk"
        visuals = @(
            (New-SlicerVisual "slicerEnvEndpt" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-SlicerVisual "slicerHttpType" 210 20 170 80 55 "AppConnectorRefs" "HttpConnectorType" "HTTP Connector Type")
            (New-CardVisual "cardEndptHttpApp" 400 20 105 80 100 "AppConnectorRefs" "App HTTP Connector Refs" "App HTTP Refs")
            (New-CardVisual "cardEndptHttpRaw" 520 20 105 80 200 "AppConnectorRefs" "App HTTP Raw Refs" "HTTP (Raw)")
            (New-CardVisual "cardEndptHttpEntra" 640 20 105 80 300 "AppConnectorRefs" "App HTTP Entra Refs" "HTTP (Entra)")
            (New-CardVisual "cardEndptFlowHttp" 760 20 105 80 400 "FlowActions" "Flow HTTP Actions" "Flow HTTP")
            (New-BarChartVisual "barTopConnectors" 20 120 430 240 1000 "AppConnectorRefs" "DisplayName" "Total Connector References" "Top Connectors")
            (New-TreemapVisual "tmActionTypes" 470 120 430 240 2000 "FlowActions" "ActionType" "FlowActions" "Total Flow Actions" "Action Types")
            (New-TableVisual "tblAppEndpoints" 20 380 880 160 3000 "AppConnectorRefs" @("DisplayName","ConnectorId","HttpConnectorType","EndpointUrl","DataSources") "App Endpoint Details")
            (New-TableVisual "tblFlowEndpoints" 20 555 880 160 4000 "FlowActions" @("Name","ActionType","ConnectorId","HttpConnectorType","BaseUrl","EndpointUrl","OperationId") "Flow Endpoint Details")
        )
    }
    lifecycle = @{
        displayName = "Shadow IT & Lifecycle"
        visuals = @(
            (New-SlicerVisual "slicerEnvLife" 20 20 170 80 50 "Environments" "DisplayName" "Environment")
            (New-CardVisual "cardLifeStaleApp" 210 20 120 80 100 "Apps" "Stale Apps (90d)" "Stale Apps")
            (New-CardVisual "cardLifeStaleFlow" 345 20 120 80 200 "Flows" "Stale Flows (90d)" "Stale Flows")
            (New-CardVisual "cardLifeOrphan" 480 20 120 80 300 "Apps" "Orphaned Apps" "Orphaned")
            (New-CardVisual "cardLifeUnmanaged" 615 20 120 80 400 "Flows" "Unmanaged Flows" "Unmanaged")
            (New-CardVisual "cardLifeUnsecured" 750 20 120 80 500 "Environments" "Unsecured Environments" "Unsecured Envs")
            (New-DonutVisual "donutStaleActive" 20 120 280 240 1000 "Apps" "StalenessStatus" "Total Apps" "Stale vs Active Apps")
            (New-DonutVisual "donutManagedFlow" 315 120 280 240 2000 "Flows" "ManagedStatus" "Total Flows" "Flow Lifecycle")
            (New-BarChartVisual "barTopCreators" 610 120 280 240 3000 "Flows" "CreatorDisplayName" "Total Flows" "Top Creators")
            (New-TableVisual "tblStaleApps" 20 380 880 320 4000 "Apps" @("DisplayName","AppType","OwnerDisplayName","EnvironmentName","LastModifiedTime") "Stale App Details")
        )
    }
    # --- Detail Pages ---
    "app-details" = @{
        displayName = "App Details"
        visuals = @(
            (New-SlicerVisual "slicerAppDetail" 20 20 170 80 50 "Apps" "DisplayName" "Select App")
            (New-TableVisual "tblAppInfo" 20 120 880 280 1000 "Apps" @("DisplayName","AppType","Status","OwnerDisplayName","OwnerEmail","EnvironmentName","SharedUsersCount","SharedGroupsCount","UsesPremiumApi","UsesCustomApi","IsSolutionAware","BypassConsent","CreatedTime","LastModifiedTime") "App Info")
            (New-TableVisual "tblAppConnectors" 20 420 880 280 2000 "AppConnectorRefs" @("DisplayName","ConnectorId","EndpointUrl","DataSources") "App Connectors")
        )
    }
    "flow-details" = @{
        displayName = "Flow Details"
        visuals = @(
            (New-SlicerVisual "slicerFlowDetail" 20 20 170 80 50 "Flows" "DisplayName" "Select Flow")
            (New-TableVisual "tblFlowInfo" 20 120 880 200 1000 "Flows" @("DisplayName","State","CreatorDisplayName","TriggerType","EnvironmentName","IsSolutionAware","IsManaged","SuspensionReason","CreatedTime","LastModifiedTime") "Flow Info")
            (New-TableVisual "tblFlowActions" 20 340 880 180 2000 "FlowActions" @("Name","ActionType","ConnectorId","BaseUrl","EndpointUrl","OperationId") "Flow Actions")
            (New-TableVisual "tblFlowTriggers" 20 535 880 180 3000 "FlowTriggers" @("Name","TriggerType","ConnectorId","BaseUrl","EndpointUrl","OperationId") "Flow Triggers")
        )
    }
    "env-details" = @{
        displayName = "Environment Details"
        visuals = @(
            (New-SlicerVisual "slicerEnvDetail" 20 20 170 80 50 "Environments" "DisplayName" "Select Environment")
            (New-CardVisual "cardEnvDetApps" 210 20 120 80 100 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardEnvDetFlows" 345 20 120 80 200 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardEnvDetConn" 480 20 120 80 300 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardEnvDetUnsec" 615 20 120 80 400 "Environments" "Unsecured Environments" "Unsecured")
            (New-CardVisual "cardEnvDetCap" 750 20 120 80 500 "Environments" "Total Capacity GB" "Capacity GB")
            (New-TableVisual "tblEnvInfo" 20 120 880 200 1000 "Environments" @("DisplayName","EnvironmentType","Region","State","IsDefault","IsDataverseEnabled","SecurityGroupId","DatabaseUsedMb","FileUsedMb","LogUsedMb","CreatedTime","LastModifiedTime") "Environment Info")
            (New-TableVisual "tblEnvApps" 20 340 430 360 2000 "Apps" @("DisplayName","AppType","OwnerDisplayName","Status") "Environment Apps")
            (New-TableVisual "tblEnvFlows" 470 340 430 360 3000 "Flows" @("DisplayName","State","TriggerType","CreatorDisplayName") "Environment Flows")
        )
    }
    "dlp-details" = @{
        displayName = "DLP Policy Details"
        visuals = @(
            (New-SlicerVisual "slicerDlpDetail" 20 20 170 80 50 "DlpPolicies" "DisplayName" "Select Policy")
            (New-CardVisual "cardDlpDetRules" 210 20 120 80 100 "DlpConnectorRules" "Total Connector Rules" "Rules")
            (New-CardVisual "cardDlpDetBiz" 345 20 120 80 200 "DlpConnectorRules" "Business Connectors" "Business")
            (New-CardVisual "cardDlpDetBlocked" 480 20 120 80 300 "DlpConnectorRules" "Blocked Connectors" "Blocked")
            (New-TableVisual "tblDlpInfo" 20 120 880 200 1000 "DlpPolicies" @("DisplayName","IsEnabled","PolicyType","EnvironmentScope","CreatedTime","LastModifiedTime") "Policy Info")
            (New-TableVisual "tblDlpConnRules" 20 340 880 360 2000 "DlpConnectorRules" @("ConnectorName","Classification","PolicyName") "Connector Rules")
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
Write-Host " Enterprise Governance PBIP created!" -ForegroundColor Green
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
Write-Host "12 Pages (8 governance + 4 detail):" -ForegroundColor Yellow
Write-Host "  1. Executive Summary  — governance score, KPI gauges, environment treemap" -ForegroundColor Gray
Write-Host "  2. Environment Gov.   — security posture, capacity, type distribution" -ForegroundColor Gray
Write-Host "  3. App Inventory/Risk — staleness, orphans, sharing risk, bypass consent" -ForegroundColor Gray
Write-Host "  4. Flow Health        — suspension analysis, trigger patterns, reliability" -ForegroundColor Gray
Write-Host "  5. Connector Risk     — tier exposure, premium cost, top connectors treemap" -ForegroundColor Gray
Write-Host "  6. DLP & Compliance   — policy coverage, classification, blocked connectors" -ForegroundColor Gray
Write-Host "  7. Endpoint/API Risk  — connector endpoints, flow action URLs, connection targets" -ForegroundColor Gray
Write-Host "  8. Shadow IT/Lifecycle— stale assets, unmanaged flows, top creators" -ForegroundColor Gray
Write-Host "  9. App Details        — select app, view all properties + connectors" -ForegroundColor Gray
Write-Host " 10. Flow Details       — select flow, view info + actions + triggers" -ForegroundColor Gray
Write-Host " 11. Environment Details— select env, view info + apps + flows" -ForegroundColor Gray
Write-Host " 12. DLP Policy Details — select policy, view info + connector rules" -ForegroundColor Gray
Write-Host ""
Write-Host "Key Governance Measures:" -ForegroundColor Yellow
Write-Host "  - Governance Score (weighted composite of security, suspension, staleness, DLP)" -ForegroundColor Gray
Write-Host "  - Env Security Rate, Suspension Rate, Stale App/Flow Rate" -ForegroundColor Gray
Write-Host "  - Orphaned Apps, Bypass Consent, Widely Shared, Unmanaged Flows" -ForegroundColor Gray
Write-Host "  - DLP Coverage Rate, Blocked Connector Rate, Connector Utilization" -ForegroundColor Gray
Write-Host ""
Write-Host "Relationships (auto-configured):" -ForegroundColor Yellow
Write-Host "  Apps -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  Flows -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  Connectors -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  DlpConnectorRules -> DlpPolicies (PolicyId)" -ForegroundColor Gray
Write-Host "  UsageAnalytics -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host ""
