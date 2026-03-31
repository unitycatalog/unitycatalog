# TablesApi

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog/delta/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](TablesApi.md#createStagingTable) | **POST** /catalogs/{catalog}/schemas/{schema}/staging-tables | Create a staging table |
| [**createTable**](TablesApi.md#createTable) | **POST** /catalogs/{catalog}/schemas/{schema}/tables | Create a table |
| [**deleteTable**](TablesApi.md#deleteTable) | **DELETE** /catalogs/{catalog}/schemas/{schema}/tables/{table} | Delete a table |
| [**listTables**](TablesApi.md#listTables) | **GET** /catalogs/{catalog}/schemas/{schema}/tables | List tables |
| [**loadTable**](TablesApi.md#loadTable) | **GET** /catalogs/{catalog}/schemas/{schema}/tables/{table} | Load table metadata |
| [**renameTable**](TablesApi.md#renameTable) | **POST** /catalogs/{catalog}/tables/rename | Rename a table |
| [**reportMetrics**](TablesApi.md#reportMetrics) | **POST** /catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics | Report commit metrics |
| [**tableExists**](TablesApi.md#tableExists) | **HEAD** /catalogs/{catalog}/schemas/{schema}/tables/{table} | Check if table exists |
| [**updateTable**](TablesApi.md#updateTable) | **POST** /catalogs/{catalog}/schemas/{schema}/tables/{table} | Update table |


<a name="createStagingTable"></a>
# **createStagingTable**
> StagingTableResponse createStagingTable(catalog, schema, CreateStagingTableRequest)

Create a staging table

    Create a staging table that will become a catalog managed table. The server allocates a table UUID and a storage location for the table. This is a new endpoint for Delta only. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **CreateStagingTableRequest** | [**CreateStagingTableRequest**](../Models/CreateStagingTableRequest.md)|  | |

### Return type

[**StagingTableResponse**](../Models/StagingTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="createTable"></a>
# **createTable**
> LoadTableResponse createTable(catalog, schema, CreateTableRequest)

Create a table

    Create a new Delta table in the specified schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **CreateTableRequest** | [**CreateTableRequest**](../Models/CreateTableRequest.md)|  | |

### Return type

[**LoadTableResponse**](../Models/LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteTable"></a>
# **deleteTable**
> deleteTable(catalog, schema, table)

Delete a table

    Delete a table from the catalog. 

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

<a name="listTables"></a>
# **listTables**
> ListTablesResponse listTables(catalog, schema, maxResults, pageToken)

List tables

    List all tables in a schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **maxResults** | **Integer**| Maximum number of tables to return | [optional] [default to 50] |
| **pageToken** | **String**| Pagination token | [optional] [default to null] |

### Return type

[**ListTablesResponse**](../Models/ListTablesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="loadTable"></a>
# **loadTable**
> LoadTableResponse loadTable(catalog, schema, table)

Load table metadata

    Load table metadata including columns, protocol, properties, and optionally credentials. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |

### Return type

[**LoadTableResponse**](../Models/LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="renameTable"></a>
# **renameTable**
> renameTable(catalog, RenameTableRequest)

Rename a table

    Rename a table. Can only change the table name within the same schema and catalog. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **RenameTableRequest** | [**RenameTableRequest**](../Models/RenameTableRequest.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="reportMetrics"></a>
# **reportMetrics**
> reportMetrics(catalog, schema, table, ReportMetricsRequest)

Report commit metrics

    Report commit metrics (telemetry) for a table. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **ReportMetricsRequest** | [**ReportMetricsRequest**](../Models/ReportMetricsRequest.md)|  | |

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
> LoadTableResponse updateTable(catalog, schema, table, UpdateTableRequest)

Update table

    Update table properties, columns, or commit Delta changes. Unlike IRC, this endpoint does not support table creation. This endpoint also covers the coordinated commit flow described in the Unity Catalog Managed Tables Specification. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **UpdateTableRequest** | [**UpdateTableRequest**](../Models/UpdateTableRequest.md)|  | |

### Return type

[**LoadTableResponse**](../Models/LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

