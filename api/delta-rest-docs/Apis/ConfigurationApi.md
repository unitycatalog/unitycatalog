# ConfigurationApi

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog/delta/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getConfig**](ConfigurationApi.md#getConfig) | **GET** /config | Get catalog configuration |


<a name="getConfig"></a>
# **getConfig**
> CatalogConfig getConfig(catalog, protocol-versions)

Get catalog configuration

    Get catalog configuration and supported endpoints. This endpoint also serves as the API versioning mechanism: the client sends its supported versions via &#x60;protocol-versions&#x60;, and the server returns versioned endpoints for the newest version both sides support. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **protocol-versions** | **String**| Comma-separated list of highest protocol versions the client supports per major version (e.g., 1.1,2.3 means the client supports 1.0-1.1 and 2.0-2.3). The server selects the highest mutually supported protocol version and returns endpoints for that version.  | [default to null] |

### Return type

[**CatalogConfig**](../Models/CatalogConfig.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

