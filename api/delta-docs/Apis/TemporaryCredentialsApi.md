# TemporaryCredentialsApi

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getStagingTableCredentials**](TemporaryCredentialsApi.md#getStagingTableCredentials) | **GET** /delta/v1/staging-tables/{table_id}/credentials | Get staging table credentials by UUID |
| [**getTableCredentials**](TemporaryCredentialsApi.md#getTableCredentials) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials | Get table credentials |
| [**getTemporaryPathCredentials**](TemporaryCredentialsApi.md#getTemporaryPathCredentials) | **GET** /delta/v1/temporary-path-credentials | Get temporary path credentials |


<a name="getStagingTableCredentials"></a>
# **getStagingTableCredentials**
> CredentialsResponse getStagingTableCredentials(table\_id)

Get staging table credentials by UUID

    Get temporary credentials for accessing staging table data by UUID. The staging table is identified solely by its UUID -- catalog and schema are not needed since the UUID is globally unique. Credentials are always READ_WRITE since the client needs to write the initial commit. This is a new endpoint for Delta only. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **table\_id** | **UUID**| Staging table UUID | [default to null] |

### Return type

[**CredentialsResponse**](../Models/CredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getTableCredentials"></a>
# **getTableCredentials**
> CredentialsResponse getTableCredentials(operation, catalog, schema, table)

Get table credentials

    Get temporary credentials for accessing table data (vended credentials). The operation parameter controls the access level. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **operation** | [**CredentialOperation**](../Models/.md)| The operation the credential is scoped to. | [default to null] [enum: READ, READ_WRITE] |
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |

### Return type

[**CredentialsResponse**](../Models/CredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getTemporaryPathCredentials"></a>
# **getTemporaryPathCredentials**
> CredentialsResponse getTemporaryPathCredentials(location, operation)

Get temporary path credentials

    Get temporary credentials of a storage path for creating a new external table. This path will later be registered in UC as a real external table. The URL has no catalog or schema because the path isn&#39;t part of any catalog or namespace. This is a new endpoint for Delta only. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **location** | **String**| Storage path for the external table | [default to null] |
| **operation** | [**CredentialOperation**](../Models/.md)| Operation type for the path credential. | [default to null] [enum: READ, READ_WRITE] |

### Return type

[**CredentialsResponse**](../Models/CredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

