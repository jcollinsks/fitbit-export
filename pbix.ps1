<#
.SYNOPSIS
    Generates a Power BI Project (PBIP) from Power Platform CSV exports.
.DESCRIPTION
    Creates a complete PBIP project with:
    - Semantic model (tables, relationships, DAX measures)
    - 6 report pages: Environments, Apps, Flows, Connectors, DLP, Usage Analytics
    - CSV data source with configurable folder path parameter

    Open the generated .pbip file in Power BI Desktop (Developer Mode enabled).
.PARAMETER CsvPath
    Path to the folder containing CSV files from Collect-PowerPlatformData.ps1.
.PARAMETER OutputPath
    Where to create the PBIP project folder. Defaults to ./PowerPlatformReport.
.EXAMPLE
    .\New-PowerPlatformPbiReport.ps1 -CsvPath C:\exports\PowerPlatformExport
    .\New-PowerPlatformPbiReport.ps1 -CsvPath .\PowerPlatformExport -OutputPath .\MyReport
#>

param(
    [Parameter(Mandatory)]
    [string]$CsvPath,
    [string]$OutputPath = "./PowerPlatformReport"
)

$ErrorActionPreference = "Stop"

# Resolve to absolute path with trailing backslash
$CsvPath = (Resolve-Path $CsvPath).Path.TrimEnd('\') + '\'

# ============================================================================
# HELPERS
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
# PROJECT STRUCTURE
# ============================================================================

$projectName = "PowerPlatformGovernance"
$reportDir = "$OutputPath/$projectName.Report"
$modelDir = "$OutputPath/$projectName.SemanticModel"

# Clean up ALL old output to avoid stale files (close Power BI Desktop first!)
Write-Host "Cleaning old output..." -ForegroundColor Yellow
Get-ChildItem -Path $OutputPath -Directory -Filter "*.Report" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
Get-ChildItem -Path $OutputPath -Directory -Filter "*.SemanticModel" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
# Remove .pbi cache folders that Power BI Desktop creates
Get-ChildItem -Path $OutputPath -Directory -Filter ".pbi" -Recurse -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force

Write-Host "Creating PBIP project at: $OutputPath" -ForegroundColor Cyan

# Root .pbip file
Write-JsonFile "$OutputPath/$projectName.pbip" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json"
    version = "1.0"
    artifacts = @(@{ report = @{ path = "$projectName.Report" } })
    settings = @{ enableAutoRecovery = $true }
})

# .gitignore
Set-Content "$OutputPath/.gitignore" @"
**/.pbi/localSettings.json
**/.pbi/cache.abf
"@ -Encoding UTF8

# ============================================================================
# SEMANTIC MODEL (model.bim)
# ============================================================================

Write-Host "Building semantic model..." -ForegroundColor Yellow

# --- Table definitions ---

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
    )
}

$tApps = [ordered]@{
    name = "Apps"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "AppId")
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
        (New-MeasureDef "Canvas Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[AppType] = `"CanvasApp`")")
        (New-MeasureDef "Model-Driven Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[AppType] = `"ModelDrivenApp`")")
        (New-MeasureDef "Premium API Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[UsesPremiumApi] = TRUE())")
        (New-MeasureDef "Solution-Aware Apps" "CALCULATE(COUNTROWS('Apps'), 'Apps'[IsSolutionAware] = TRUE())")
        (New-MeasureDef "Total Shared Users" "SUM('Apps'[SharedUsersCount])")
        (New-MeasureDef "Avg Shared Users" "AVERAGE('Apps'[SharedUsersCount])" "#,##0.0")
    )
}

$tFlows = [ordered]@{
    name = "Flows"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "FlowId")
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
        (New-MeasureDef "Premium Connectors" "CALCULATE(COUNTROWS('Connectors'), 'Connectors'[Tier] = `"Premium`")")
    )
}

$tConnections = [ordered]@{
    name = "Connections"; lineageTag = (New-Guid)
    columns = @(
        (New-ColumnDef "ConnectionId")
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
        @{Name="CreatedByObjectId"; Type="type text"},
        @{Name="CreatedByName"; Type="type text"}, @{Name="CreatedByEmail"; Type="type text"},
        @{Name="CreatedTime"; Type="type datetime"}, @{Name="Status"; Type="type text"},
        @{Name="IsShared"; Type="type logical"}, @{Name="CollectedAt"; Type="type datetime"}
    )))
    measures = @(
        (New-MeasureDef "Total Connections" "COUNTROWS('Connections')")
        (New-MeasureDef "Error Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[Status] = `"Error`")")
        (New-MeasureDef "Shared Connections" "CALCULATE(COUNTROWS('Connections'), 'Connections'[IsShared] = TRUE())")
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
        (New-MeasureDef "Business Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"Business`")" "#,##0" "DLP")
        (New-MeasureDef "Non-Business Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"NonBusiness`")" "#,##0" "DLP")
        (New-MeasureDef "Blocked Connectors" "CALCULATE(COUNTROWS('DlpConnectorRules'), 'DlpConnectorRules'[Classification] = `"Blocked`")" "#,##0" "DLP")
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
    )
    partitions = @((New-CsvPartition "AppConnectorRefs" @(
        @{Name="AppId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="DataSources"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Connector References" "COUNTROWS('AppConnectorRefs')")
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
    )
    partitions = @((New-CsvPartition "FlowActions" @(
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="ActionType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Flow Actions" "COUNTROWS('FlowActions')")
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
    )
    partitions = @((New-CsvPartition "FlowTriggers" @(
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="Name"; Type="type text"},
        @{Name="TriggerType"; Type="type text"}, @{Name="ConnectorId"; Type="type text"},
        @{Name="OperationId"; Type="type text"}, @{Name="EndpointUrl"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Flow Triggers" "COUNTROWS('FlowTriggers')")
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
    )
}

# --- Build model.bim ---

$modelBim = [ordered]@{
    compatibilityLevel = 1567
    model = [ordered]@{
        culture = "en-US"
        defaultPowerBIDataSourceVersion = "powerBI_V3"
        sourceQueryCulture = "en-US"
        tables = @($tEnvironments, $tApps, $tFlows, $tConnectors, $tConnections,
                    $tDlpPolicies, $tDlpRules, $tUsage, $tAppConnRefs, $tFlowActions, $tFlowTriggers, $tFlowConnRefs)
        relationships = @(
            (New-RelationshipDef "rel_Apps_Env" "Apps" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Flows_Env" "Flows" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connectors_Env" "Connectors" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connections_Env" "Connections" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_DlpRules_Policy" "DlpConnectorRules" "PolicyId" "DlpPolicies" "PolicyId")
            (New-RelationshipDef "rel_Usage_Env" "UsageAnalytics" "EnvironmentId" "Environments" "EnvironmentId")
        )
        expressions = @(
            [ordered]@{
                name = "CsvFolderPath"
                kind = "m"
                expression = @("`"$($CsvPath -replace '\\', '\\')`" meta [IsParameterQuery=true, Type=`"Text`", IsParameterQueryRequired=true]")
            }
        )
        annotations = @(
            @{ name = "PBI_QueryOrder"; value = "[`"Environments`",`"Apps`",`"Flows`",`"Connectors`",`"Connections`",`"DlpPolicies`",`"DlpConnectorRules`",`"UsageAnalytics`",`"AppConnectorRefs`",`"FlowActions`",`"FlowTriggers`",`"FlowConnectionRefs`"]" }
            @{ name = "__PBI_TimeIntelligenceEnabled"; value = "0" }
        )
    }
}

# Write semantic model files
Write-JsonFile "$modelDir/definition.pbism" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json"
    version = "4.0"
})
Write-JsonFile "$modelDir/model.bim" $modelBim

# ============================================================================
# REPORT DEFINITION (PBIR format)
# ============================================================================

Write-Host "Building report pages..." -ForegroundColor Yellow

# Report entry point
Write-JsonFile "$reportDir/definition.pbir" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json"
    version = "4.0"
    datasetReference = @{ byPath = @{ path = "../$projectName.SemanticModel" } }
})

# Report definition files
$defDir = "$reportDir/definition"
Write-JsonFile "$defDir/version.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json"
    version = "2.0.0"
})
# Write report.json as raw JSON to avoid PowerShell hashtable serialization issues
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

$pageNames = @("environments", "apps", "flows", "connectors", "dlp", "usage")
Write-JsonFile "$defDir/pages/pages.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json"
    pageOrder = $pageNames
    activePageName = "environments"
})

# --- Page definitions ---

$pageDefs = @{
    environments = @{
        displayName = "Environments"
        visuals = @(
            (New-CardVisual "cardTotalEnv" 20 20 145 100 100 "Environments" "Total Environments" "Environments")
            (New-CardVisual "cardProdEnv" 185 20 145 100 200 "Environments" "Production Environments" "Production")
            (New-CardVisual "cardSandboxEnv" 350 20 145 100 300 "Environments" "Sandbox Environments" "Sandbox")
            (New-CardVisual "cardDataverse" 515 20 145 100 400 "Environments" "Dataverse Enabled" "Dataverse")
            (New-CardVisual "cardCapacity" 680 20 145 100 500 "Environments" "Total Capacity GB" "Capacity (GB)")
            (New-DonutVisual "donutEnvType" 20 140 400 260 1000 "Environments" "EnvironmentType" "Total Environments" "Environment Types")
            (New-BarChartVisual "barCapacity" 440 140 400 260 2000 "Environments" "DisplayName" "Total Capacity GB" "Capacity by Environment")
            (New-TableVisual "tblEnvs" 20 420 820 280 3000 "Environments" @("DisplayName","EnvironmentType","Region","IsDataverseEnabled","DatabaseUsedMb","FileUsedMb") "Environment Details")
        )
    }
    apps = @{
        displayName = "Apps"
        visuals = @(
            (New-CardVisual "cardTotalApps" 20 20 145 100 100 "Apps" "Total Apps" "Apps")
            (New-CardVisual "cardCanvas" 185 20 145 100 200 "Apps" "Canvas Apps" "Canvas")
            (New-CardVisual "cardModelDriven" 350 20 145 100 300 "Apps" "Model-Driven Apps" "Model-Driven")
            (New-CardVisual "cardPremium" 515 20 145 100 400 "Apps" "Premium API Apps" "Premium")
            (New-CardVisual "cardSolutionApps" 680 20 145 100 500 "Apps" "Solution-Aware Apps" "Solution-Aware")
            (New-DonutVisual "donutAppType" 20 140 400 260 1000 "Apps" "AppType" "Total Apps" "App Types")
            (New-BarChartVisual "barAppsByEnv" 440 140 400 260 2000 "Apps" "EnvironmentName" "Total Apps" "Apps by Environment")
            (New-TableVisual "tblApps" 20 420 820 280 3000 "Apps" @("DisplayName","AppType","OwnerDisplayName","EnvironmentName","SharedUsersCount","LastModifiedTime") "App Details")
        )
    }
    flows = @{
        displayName = "Flows"
        visuals = @(
            (New-CardVisual "cardTotalFlows" 20 20 145 100 100 "Flows" "Total Flows" "Flows")
            (New-CardVisual "cardActive" 185 20 145 100 200 "Flows" "Active Flows" "Active")
            (New-CardVisual "cardSuspended" 350 20 145 100 300 "Flows" "Suspended Flows" "Suspended")
            (New-CardVisual "cardStopped" 515 20 145 100 400 "Flows" "Stopped Flows" "Stopped")
            (New-CardVisual "cardManagedFlows" 680 20 145 100 500 "Flows" "Managed Flows" "Managed")
            (New-DonutVisual "donutFlowState" 20 140 400 260 1000 "Flows" "State" "Total Flows" "Flow States")
            (New-BarChartVisual "barFlowsByEnv" 440 140 400 260 2000 "Flows" "EnvironmentName" "Total Flows" "Flows by Environment")
            (New-TableVisual "tblFlows" 20 420 820 140 3000 "Flows" @("DisplayName","State","CreatorDisplayName","TriggerType","EnvironmentName","LastModifiedTime") "Flow Details")
            (New-TableVisual "tblFlowActions" 20 580 820 120 4000 "FlowActions" @("Name","ActionType","ConnectorId","OperationId","EndpointUrl") "Flow Actions")
        )
    }
    connectors = @{
        displayName = "Connectors"
        visuals = @(
            (New-CardVisual "cardTotalConn" 20 20 145 100 100 "Connectors" "Total Connectors" "Connectors")
            (New-CardVisual "cardCustomConn" 185 20 145 100 200 "Connectors" "Custom Connectors" "Custom")
            (New-CardVisual "cardPremConn" 350 20 145 100 300 "Connectors" "Premium Connectors" "Premium")
            (New-CardVisual "cardTotalConns" 515 20 145 100 400 "Connections" "Total Connections" "Connections")
            (New-CardVisual "cardErrorConns" 680 20 145 100 500 "Connections" "Error Connections" "Errors")
            (New-DonutVisual "donutConnTier" 20 140 400 260 1000 "Connectors" "Tier" "Total Connectors" "Connector Tiers")
            (New-DonutVisual "donutConnStatus" 440 140 400 260 2000 "Connections" "Status" "Total Connections" "Connection Status")
            (New-TableVisual "tblConnectors" 20 420 820 140 3000 "Connectors" @("DisplayName","Tier","Publisher","IsCustom","EnvironmentName") "Connector Details")
            (New-TableVisual "tblConnUrls" 20 580 820 120 4000 "Connections" @("DisplayName","ConnectionUrl","Status","CreatedByName","EnvironmentName") "Connections with URLs")
        )
    }
    dlp = @{
        displayName = "DLP Policies"
        visuals = @(
            (New-CardVisual "cardTotalDlp" 20 20 145 100 100 "DlpPolicies" "Total DLP Policies" "DLP Policies")
            (New-CardVisual "cardEnabledDlp" 185 20 145 100 200 "DlpPolicies" "Enabled Policies" "Enabled")
            (New-CardVisual "cardBizConn" 350 20 145 100 300 "DlpConnectorRules" "Business Connectors" "Business")
            (New-CardVisual "cardNonBizConn" 515 20 145 100 400 "DlpConnectorRules" "Non-Business Connectors" "Non-Business")
            (New-CardVisual "cardBlockedConn" 680 20 145 100 500 "DlpConnectorRules" "Blocked Connectors" "Blocked")
            (New-DonutVisual "donutDlpClass" 20 140 400 260 1000 "DlpConnectorRules" "Classification" "Blocked Connectors" "Classification Breakdown")
            (New-TableVisual "tblDlpPolicies" 440 140 400 260 2000 "DlpPolicies" @("DisplayName","IsEnabled","EnvironmentScope","LastModifiedTime") "DLP Policies")
            (New-TableVisual "tblDlpRules" 20 420 820 280 3000 "DlpConnectorRules" @("PolicyName","ConnectorName","Classification") "Connector Rules")
        )
    }
    usage = @{
        displayName = "Usage Analytics"
        visuals = @(
            (New-CardVisual "cardUniqueUsers" 20 20 145 100 100 "UsageAnalytics" "Total Unique Users" "Users")
            (New-CardVisual "cardSessions" 185 20 145 100 200 "UsageAnalytics" "Total Sessions" "Sessions")
            (New-CardVisual "cardActions" 350 20 145 100 300 "UsageAnalytics" "Total Actions" "Actions")
            (New-BarChartVisual "barUsageByType" 20 140 400 260 1000 "UsageAnalytics" "ResourceType" "Total Sessions" "Sessions by Resource Type")
            (New-TableVisual "tblUsage" 440 140 400 260 2000 "UsageAnalytics" @("ResourceType","EnvironmentId","Date","UniqueUsers","TotalSessions","TotalActions") "Usage Details")
        )
    }
}

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
Write-Host " PBIP project created!" -ForegroundColor Green
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
Write-Host "Pages:" -ForegroundColor Yellow
Write-Host "  1. Environments — type distribution, capacity, Dataverse status" -ForegroundColor Gray
Write-Host "  2. Apps — canvas vs model-driven, premium API usage, sharing" -ForegroundColor Gray
Write-Host "  3. Flows — active/suspended/stopped, trigger types, by environment" -ForegroundColor Gray
Write-Host "  4. Connectors — tier breakdown, connection status, custom connectors" -ForegroundColor Gray
Write-Host "  5. DLP Policies — business/non-business/blocked classification" -ForegroundColor Gray
Write-Host "  6. Usage Analytics — unique users, sessions, actions by resource type" -ForegroundColor Gray
Write-Host ""
Write-Host "Relationships (auto-configured):" -ForegroundColor Yellow
Write-Host "  Apps -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  Flows -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  Connectors -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  Connections -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  DlpConnectorRules -> DlpPolicies (PolicyId)" -ForegroundColor Gray
Write-Host "  UsageAnalytics -> Environments (EnvironmentId)" -ForegroundColor Gray
Write-Host "  AppConnectorRefs -> Apps (AppId)" -ForegroundColor Gray
Write-Host "  FlowActions -> Flows (FlowId)" -ForegroundColor Gray
Write-Host "  FlowTriggers -> Flows (FlowId)" -ForegroundColor Gray
Write-Host ""
