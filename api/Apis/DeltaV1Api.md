# DeltaV1Api

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**deltaV1Config**](DeltaV1Api.md#deltaV1Config) | **GET** /delta/v1/config | Discover the Delta v1 managed table API. |
| [**deltaV1CreateStagingTable**](DeltaV1Api.md#deltaV1CreateStagingTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables | Create a managed Delta staging table. |
| [**deltaV1CreateTable**](DeltaV1Api.md#deltaV1CreateTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables | Finalize creation of a managed Delta table. |
| [**deltaV1GetStagingTableCredentials**](DeltaV1Api.md#deltaV1GetStagingTableCredentials) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables/{table_id}/credentials | Generate temporary credentials for a Delta staging table. |
| [**deltaV1GetTableCredentials**](DeltaV1Api.md#deltaV1GetTableCredentials) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials | Generate temporary credentials for a managed Delta table. |
| [**deltaV1LoadTable**](DeltaV1Api.md#deltaV1LoadTable) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Load a managed Delta table. |
| [**deltaV1UpdateTable**](DeltaV1Api.md#deltaV1UpdateTable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Update metadata or commit a managed Delta table version. |


<a name="deltaV1Config"></a>
# **deltaV1Config**
> DeltaV1ConfigResponse deltaV1Config(catalog, protocol\_versions)

Discover the Delta v1 managed table API.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [optional] [default to null] |
| **protocol\_versions** | **String**|  | [optional] [default to null] |

### Return type

[**DeltaV1ConfigResponse**](../Models/DeltaV1ConfigResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deltaV1CreateStagingTable"></a>
# **deltaV1CreateStagingTable**
> DeltaV1StagingTableResponse deltaV1CreateStagingTable(catalog, schema, DeltaV1CreateStagingTableRequest)

Create a managed Delta staging table.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **DeltaV1CreateStagingTableRequest** | [**DeltaV1CreateStagingTableRequest**](../Models/DeltaV1CreateStagingTableRequest.md)|  | |

### Return type

[**DeltaV1StagingTableResponse**](../Models/DeltaV1StagingTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deltaV1CreateTable"></a>
# **deltaV1CreateTable**
> DeltaV1LoadTableResponse deltaV1CreateTable(catalog, schema, DeltaV1CreateTableRequest)

Finalize creation of a managed Delta table.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **DeltaV1CreateTableRequest** | [**DeltaV1CreateTableRequest**](../Models/DeltaV1CreateTableRequest.md)|  | |

### Return type

[**DeltaV1LoadTableResponse**](../Models/DeltaV1LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deltaV1GetStagingTableCredentials"></a>
# **deltaV1GetStagingTableCredentials**
> TemporaryCredentials deltaV1GetStagingTableCredentials(catalog, schema, table\_id, operation)

Generate temporary credentials for a Delta staging table.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **table\_id** | **String**|  | [default to null] |
| **operation** | [**TableOperation**](../Models/.md)|  | [optional] [default to null] [enum: UNKNOWN_TABLE_OPERATION, READ, READ_WRITE] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deltaV1GetTableCredentials"></a>
# **deltaV1GetTableCredentials**
> TemporaryCredentials deltaV1GetTableCredentials(catalog, schema, table, operation)

Generate temporary credentials for a managed Delta table.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **table** | **String**|  | [default to null] |
| **operation** | [**TableOperation**](../Models/.md)|  | [optional] [default to null] [enum: UNKNOWN_TABLE_OPERATION, READ, READ_WRITE] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deltaV1LoadTable"></a>
# **deltaV1LoadTable**
> DeltaV1LoadTableResponse deltaV1LoadTable(catalog, schema, table, start\_version, end\_version)

Load a managed Delta table.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **table** | **String**|  | [default to null] |
| **start\_version** | **Long**|  | [optional] [default to null] |
| **end\_version** | **Long**|  | [optional] [default to null] |

### Return type

[**DeltaV1LoadTableResponse**](../Models/DeltaV1LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deltaV1UpdateTable"></a>
# **deltaV1UpdateTable**
> DeltaV1LoadTableResponse deltaV1UpdateTable(catalog, schema, table, DeltaV1UpdateTableRequest)

Update metadata or commit a managed Delta table version.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**|  | [default to null] |
| **schema** | **String**|  | [default to null] |
| **table** | **String**|  | [default to null] |
| **DeltaV1UpdateTableRequest** | [**DeltaV1UpdateTableRequest**](../Models/DeltaV1UpdateTableRequest.md)|  | |

### Return type

[**DeltaV1LoadTableResponse**](../Models/DeltaV1LoadTableResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

