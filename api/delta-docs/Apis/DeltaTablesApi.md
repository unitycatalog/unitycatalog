# DeltaTablesApi

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](DeltaTablesApi.md#createStagingTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables | Create a staging table |
| [**createTable**](DeltaTablesApi.md#createTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables | Create a table |
| [**deleteTable**](DeltaTablesApi.md#deleteTable) | **DELETE** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Delete a table |
| [**loadTable**](DeltaTablesApi.md#loadTable) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Load table metadata |
| [**renameTable**](DeltaTablesApi.md#renameTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename | Rename a table |
| [**reportMetrics**](DeltaTablesApi.md#reportMetrics) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics | Report commit metrics |
| [**tableExists**](DeltaTablesApi.md#tableExists) | **HEAD** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Check if table exists |
| [**updateTable**](DeltaTablesApi.md#updateTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Update table |


<a name="createStagingTable"></a>
# **createStagingTable**
> DeltaStagingTableResponse createStagingTable(catalog, schema, DeltaCreateStagingTableRequest)

Create a staging table

    Create a staging table that will become a catalog managed table. The server allocates a table UUID and a storage location for the table. This is a new endpoint for Delta only. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **DeltaCreateStagingTableRequest** | [**DeltaCreateStagingTableRequest**](../Models/DeltaCreateStagingTableRequest.md)|  | |

### Return type

[**DeltaStagingTableResponse**](../Models/DeltaStagingTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="createTable"></a>
# **createTable**
> DeltaLoadTableResponse createTable(catalog, schema, DeltaCreateTableRequest)

Create a table

    Create a new Delta table in the specified schema. Supports MANAGED, EXTERNAL, and MANAGED_SHALLOW_CLONE tables. EXTERNAL_SHALLOW_CLONE is not yet supported. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **DeltaCreateTableRequest** | [**DeltaCreateTableRequest**](../Models/DeltaCreateTableRequest.md)|  | |

### Return type

[**DeltaLoadTableResponse**](../Models/DeltaLoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteTable"></a>
# **deleteTable**
> deleteTable(catalog, schema, table)

Delete a table

    Delete a table from the catalog. Deleting a table that is the base of shallow clones is rejected; the clones must be deleted first. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="loadTable"></a>
# **loadTable**
> DeltaLoadTableResponse loadTable(catalog, schema, table)

Load table metadata

    Load table metadata including columns, properties, and optionally credentials. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |

### Return type

[**DeltaLoadTableResponse**](../Models/DeltaLoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="renameTable"></a>
# **renameTable**
> renameTable(catalog, schema, table, DeltaRenameTableRequest)

Rename a table

    Rename a table within the same catalog and schema. Cross-schema and cross-catalog moves are not supported. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaRenameTableRequest** | [**DeltaRenameTableRequest**](../Models/DeltaRenameTableRequest.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="reportMetrics"></a>
# **reportMetrics**
> reportMetrics(catalog, schema, table, DeltaReportMetricsRequest)

Report commit metrics

    Report commit metrics (telemetry) for a table. The path {table} and the body table-id must identify the same table; the server validates this and rejects mismatches. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaReportMetricsRequest** | [**DeltaReportMetricsRequest**](../Models/DeltaReportMetricsRequest.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="tableExists"></a>
# **tableExists**
> tableExists(catalog, schema, table)

Check if table exists

    Check if the specified table exists. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateTable"></a>
# **updateTable**
> DeltaLoadTableResponse updateTable(catalog, schema, table, DeltaUpdateTableRequest)

Update table

    Update table properties, columns, or commit Delta changes. Unlike IRC, this endpoint does not support table creation. This endpoint also covers the coordinated commit flow described in the Unity Catalog Managed Tables Specification. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaUpdateTableRequest** | [**DeltaUpdateTableRequest**](../Models/DeltaUpdateTableRequest.md)|  | |

### Return type

[**DeltaLoadTableResponse**](../Models/DeltaLoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

