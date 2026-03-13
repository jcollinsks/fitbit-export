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
    $Content | ConvertTo-Json -Depth 30 | Set-Content -Path $Path -Encoding UTF8
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
function New-SourceRef { param([string]$Alias) @{ SourceRef = @{ Source = $Alias } } }

function New-ColField {
    param([string]$Alias, [string]$Property)
    @{ Column = @{ Expression = (New-SourceRef $Alias); Property = $Property } }
}

function New-MeasureField {
    param([string]$Alias, [string]$Property)
    @{ Measure = @{ Expression = (New-SourceRef $Alias); Property = $Property } }
}

function New-Projection {
    param([string]$Table, [string]$Alias, [string]$Property, [string]$Type = "Column")
    $field = if ($Type -eq "Measure") { New-MeasureField $Alias $Property } else { New-ColField $Alias $Property }
    @{ field = $field; queryRef = "$Table.$Property"; nativeQueryRef = $Property }
}

function New-CardVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Alias, [string]$Measure)
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "card"
            query = [ordered]@{
                queryState = [ordered]@{
                    Values = @{ projections = @(,(New-Projection $Table $Alias $Measure "Measure")) }
                }
            }
            objects = @{
                labels = @(@{ properties = @{ fontSize = @{ expr = @{ Literal = @{ Value = "28D" } } } } })
                categoryLabels = @(@{ properties = @{ show = @{ expr = @{ Literal = @{ Value = "true" } } } } })
            }
            drillFilterOtherVisuals = $true
        }
    }
}

function New-BarChartVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Alias, [string]$CategoryCol, [string]$ValueMeasure)
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "clusteredBarChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $Table $Alias $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $Table $Alias $ValueMeasure "Measure")) }
                }
            }
            drillFilterOtherVisuals = $true
        }
    }
}

function New-DonutVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Alias, [string]$CategoryCol, [string]$ValueMeasure)
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "donutChart"
            query = [ordered]@{
                queryState = [ordered]@{
                    Category = @{ projections = @(,(New-Projection $Table $Alias $CategoryCol "Column")) }
                    Y = @{ projections = @(,(New-Projection $Table $Alias $ValueMeasure "Measure")) }
                }
            }
            drillFilterOtherVisuals = $true
        }
    }
}

function New-TableVisual {
    param([string]$Name, [int]$X, [int]$Y, [int]$W, [int]$H, [int]$Z,
          [string]$Table, [string]$Alias, [string[]]$Columns)
    $projections = $Columns | ForEach-Object { New-Projection $Table $Alias $_ "Column" }
    [ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"
        name = $Name
        position = [ordered]@{ x = $X; y = $Y; z = $Z; width = $W; height = $H; tabOrder = $Z }
        visual = [ordered]@{
            visualType = "tableEx"
            query = [ordered]@{
                queryState = [ordered]@{
                    Values = @{ projections = @($projections) }
                }
            }
            drillFilterOtherVisuals = $true
        }
    }
}

# ============================================================================
# PROJECT STRUCTURE
# ============================================================================

$projectName = "PowerPlatformGovernance"
$reportDir = "$OutputPath/$projectName.Report"
$modelDir = "$OutputPath/$projectName.SemanticModel"

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
        (New-ColumnDef "ConnectorId" "string" "none" -IsKey $true)
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
    )
    partitions = @((New-CsvPartition "AppConnectorRefs" @(
        @{Name="AppId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}, @{Name="DisplayName"; Type="type text"},
        @{Name="DataSources"; Type="type text"}
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
        (New-ColumnDef "ActionType")
        (New-ColumnDef "ConnectorId")
    )
    partitions = @((New-CsvPartition "FlowActions" @(
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="ActionType"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}
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
        (New-ColumnDef "TriggerType")
        (New-ColumnDef "ConnectorId")
    )
    partitions = @((New-CsvPartition "FlowTriggers" @(
        @{Name="FlowId"; Type="type text"}, @{Name="EnvironmentId"; Type="type text"},
        @{Name="Position"; Type="Int64.Type"}, @{Name="TriggerType"; Type="type text"},
        @{Name="ConnectorId"; Type="type text"}
    )))
    measures = @(
        (New-MeasureDef "Total Flow Triggers" "COUNTROWS('FlowTriggers')")
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
                    $tDlpPolicies, $tDlpRules, $tUsage, $tAppConnRefs, $tFlowActions, $tFlowTriggers)
        relationships = @(
            (New-RelationshipDef "rel_Apps_Env" "Apps" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Flows_Env" "Flows" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connectors_Env" "Connectors" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_Connections_Env" "Connections" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_DlpRules_Policy" "DlpConnectorRules" "PolicyId" "DlpPolicies" "PolicyId")
            (New-RelationshipDef "rel_Usage_Env" "UsageAnalytics" "EnvironmentId" "Environments" "EnvironmentId")
            (New-RelationshipDef "rel_AppConnRefs_App" "AppConnectorRefs" "AppId" "Apps" "AppId")
            (New-RelationshipDef "rel_FlowActions_Flow" "FlowActions" "FlowId" "Flows" "FlowId")
            (New-RelationshipDef "rel_FlowTriggers_Flow" "FlowTriggers" "FlowId" "Flows" "FlowId")
        )
        expressions = @(
            [ordered]@{
                name = "CsvFolderPath"
                kind = "m"
                expression = @("`"$($CsvPath -replace '\\', '\\')`" meta [IsParameterQuery=true, Type=`"Text`", IsParameterQueryRequired=true]")
            }
        )
        annotations = @(
            @{ name = "PBI_QueryOrder"; value = "[`"Environments`",`"Apps`",`"Flows`",`"Connectors`",`"Connections`",`"DlpPolicies`",`"DlpConnectorRules`",`"UsageAnalytics`",`"AppConnectorRefs`",`"FlowActions`",`"FlowTriggers`"]" }
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
    version = "4.0.0"
})
Write-JsonFile "$defDir/report.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json"
    layoutOptimization = "None"
    themeCollection = @{ baseTheme = @{ name = "CY24SU06"; reportVersionAtImport = "5.55"; type = "SharedResources" } }
    settings = @{ useStylableVisualContainerHeader = $true; exportDataMode = "AllowSummarizedAndUnderlying" }
})

$pageNames = @("environments", "apps", "flows", "connectors", "dlp", "usage")
Write-JsonFile "$defDir/pages.json" ([ordered]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json"
    pageOrder = $pageNames
    activePageName = "environments"
})

# --- Page definitions ---

$pageDefs = @{
    environments = @{
        displayName = "Environments"
        visuals = @(
            (New-CardVisual "cardTotalEnv" 20 20 200 120 1000 "Environments" "e" "Total Environments")
            (New-CardVisual "cardProdEnv" 240 20 200 120 1001 "Environments" "e" "Production Environments")
            (New-CardVisual "cardSandboxEnv" 460 20 200 120 1002 "Environments" "e" "Sandbox Environments")
            (New-CardVisual "cardDataverse" 680 20 200 120 1003 "Environments" "e" "Dataverse Enabled")
            (New-CardVisual "cardCapacity" 900 20 200 120 1004 "Environments" "e" "Total Capacity GB")
            (New-DonutVisual "donutEnvType" 20 160 400 300 2000 "Environments" "e" "EnvironmentType" "Total Environments")
            (New-BarChartVisual "barCapacity" 440 160 400 300 2001 "Environments" "e" "DisplayName" "Total Capacity GB")
            (New-TableVisual "tblEnvs" 20 480 860 220 3000 "Environments" "e" @("DisplayName","EnvironmentType","Region","IsDataverseEnabled","DatabaseUsedMb","FileUsedMb"))
        )
    }
    apps = @{
        displayName = "Apps"
        visuals = @(
            (New-CardVisual "cardTotalApps" 20 20 200 120 1000 "Apps" "a" "Total Apps")
            (New-CardVisual "cardCanvas" 240 20 200 120 1001 "Apps" "a" "Canvas Apps")
            (New-CardVisual "cardModelDriven" 460 20 200 120 1002 "Apps" "a" "Model-Driven Apps")
            (New-CardVisual "cardPremium" 680 20 200 120 1003 "Apps" "a" "Premium API Apps")
            (New-CardVisual "cardSolutionApps" 900 20 200 120 1004 "Apps" "a" "Solution-Aware Apps")
            (New-DonutVisual "donutAppType" 20 160 400 300 2000 "Apps" "a" "AppType" "Total Apps")
            (New-BarChartVisual "barAppsByEnv" 440 160 400 300 2001 "Apps" "a" "EnvironmentName" "Total Apps")
            (New-TableVisual "tblApps" 20 480 860 220 3000 "Apps" "a" @("DisplayName","AppType","OwnerDisplayName","EnvironmentName","SharedUsersCount","LastModifiedTime"))
        )
    }
    flows = @{
        displayName = "Flows"
        visuals = @(
            (New-CardVisual "cardTotalFlows" 20 20 200 120 1000 "Flows" "f" "Total Flows")
            (New-CardVisual "cardActive" 240 20 200 120 1001 "Flows" "f" "Active Flows")
            (New-CardVisual "cardSuspended" 460 20 200 120 1002 "Flows" "f" "Suspended Flows")
            (New-CardVisual "cardStopped" 680 20 200 120 1003 "Flows" "f" "Stopped Flows")
            (New-CardVisual "cardManagedFlows" 900 20 200 120 1004 "Flows" "f" "Managed Flows")
            (New-DonutVisual "donutFlowState" 20 160 400 300 2000 "Flows" "f" "State" "Total Flows")
            (New-BarChartVisual "barFlowsByEnv" 440 160 400 300 2001 "Flows" "f" "EnvironmentName" "Total Flows")
            (New-TableVisual "tblFlows" 20 480 860 220 3000 "Flows" "f" @("DisplayName","State","CreatorDisplayName","TriggerType","EnvironmentName","LastModifiedTime"))
        )
    }
    connectors = @{
        displayName = "Connectors"
        visuals = @(
            (New-CardVisual "cardTotalConn" 20 20 200 120 1000 "Connectors" "c" "Total Connectors")
            (New-CardVisual "cardCustomConn" 240 20 200 120 1001 "Connectors" "c" "Custom Connectors")
            (New-CardVisual "cardPremConn" 460 20 200 120 1002 "Connectors" "c" "Premium Connectors")
            (New-CardVisual "cardTotalConns" 680 20 200 120 1003 "Connections" "cx" "Total Connections")
            (New-CardVisual "cardErrorConns" 900 20 200 120 1004 "Connections" "cx" "Error Connections")
            (New-DonutVisual "donutConnTier" 20 160 400 300 2000 "Connectors" "c" "Tier" "Total Connectors")
            (New-DonutVisual "donutConnStatus" 440 160 400 300 2001 "Connections" "cx" "Status" "Total Connections")
            (New-TableVisual "tblConnectors" 20 480 860 220 3000 "Connectors" "c" @("DisplayName","Tier","Publisher","IsCustom","EnvironmentName"))
            (New-TableVisual "tblConnUrls" 20 720 860 220 3001 "Connections" "cx" @("DisplayName","ConnectionUrl","Status","CreatedByName","EnvironmentName"))
        )
    }
    dlp = @{
        displayName = "DLP Policies"
        visuals = @(
            (New-CardVisual "cardTotalDlp" 20 20 200 120 1000 "DlpPolicies" "dp" "Total DLP Policies")
            (New-CardVisual "cardEnabledDlp" 240 20 200 120 1001 "DlpPolicies" "dp" "Enabled Policies")
            (New-CardVisual "cardBizConn" 460 20 200 120 1002 "DlpConnectorRules" "dr" "Business Connectors")
            (New-CardVisual "cardNonBizConn" 680 20 200 120 1003 "DlpConnectorRules" "dr" "Non-Business Connectors")
            (New-CardVisual "cardBlockedConn" 900 20 200 120 1004 "DlpConnectorRules" "dr" "Blocked Connectors")
            (New-DonutVisual "donutDlpClass" 20 160 400 300 2000 "DlpConnectorRules" "dr" "Classification" "Blocked Connectors")
            (New-TableVisual "tblDlpPolicies" 440 160 420 300 2001 "DlpPolicies" "dp" @("DisplayName","IsEnabled","EnvironmentScope","LastModifiedTime"))
            (New-TableVisual "tblDlpRules" 20 480 860 220 3000 "DlpConnectorRules" "dr" @("PolicyName","ConnectorName","Classification"))
        )
    }
    usage = @{
        displayName = "Usage Analytics"
        visuals = @(
            (New-CardVisual "cardUniqueUsers" 20 20 200 120 1000 "UsageAnalytics" "u" "Total Unique Users")
            (New-CardVisual "cardSessions" 240 20 200 120 1001 "UsageAnalytics" "u" "Total Sessions")
            (New-CardVisual "cardActions" 460 20 200 120 1002 "UsageAnalytics" "u" "Total Actions")
            (New-BarChartVisual "barUsageByType" 20 160 400 300 2000 "UsageAnalytics" "u" "ResourceType" "Total Sessions")
            (New-TableVisual "tblUsage" 440 160 420 300 2001 "UsageAnalytics" "u" @("ResourceType","EnvironmentId","Date","UniqueUsers","TotalSessions","TotalActions"))
        )
    }
}

foreach ($pageName in $pageNames) {
    $pageDef = $pageDefs[$pageName]
    $pageDir = "$defDir/pages/$pageName"

    Write-JsonFile "$pageDir/page.json" ([ordered]@{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
        name = $pageName
        displayName = $pageDef.displayName
        displayOption = "FitToPage"
        height = 720
        width = 1280
        visibility = "AlwaysVisible"
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
