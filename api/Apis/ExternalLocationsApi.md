# ExternalLocationsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createExternalLocation**](ExternalLocationsApi.md#createExternalLocation) | **POST** /external-locations | Create an external location |
| [**deleteExternalLocation**](ExternalLocationsApi.md#deleteExternalLocation) | **DELETE** /external-locations/{name} | Delete an external location |
| [**getExternalLocation**](ExternalLocationsApi.md#getExternalLocation) | **GET** /external-locations/{name} | Get an external location |
| [**listExternalLocations**](ExternalLocationsApi.md#listExternalLocations) | **GET** /external-locations | List external locations |
| [**updateExternalLocation**](ExternalLocationsApi.md#updateExternalLocation) | **PATCH** /external-locations/{name} | Update an external location |


<a name="createExternalLocation"></a>
# **createExternalLocation**
> ExternalLocationInfo createExternalLocation(CreateExternalLocation)

Create an external location

    Creates a new external location entry in the metastore. The caller must be a metastore admin to be able to create external locations. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **CreateExternalLocation** | [**CreateExternalLocation**](../Models/CreateExternalLocation.md) | required |  |  |

### Return type

[**ExternalLocationInfo**](../Models/ExternalLocationInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteExternalLocation"></a>
# **deleteExternalLocation**
> deleteExternalLocation(name, force)

Delete an external location

    Deletes the specified external location from the metastore. The caller must be a metastore admin or the owner of the external location.

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of the external location. | |
| **force** | **Boolean** | optional | Force deletion even if there are dependent external tables or mounts. | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="getExternalLocation"></a>
# **getExternalLocation**
> ExternalLocationInfo getExternalLocation(name)

Get an external location

    Gets an external location from the metastore. The caller must be a metastore admin, the owner of the external location, or a user with some privilege on the external location. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of the external location. | |

### Return type

[**ExternalLocationInfo**](../Models/ExternalLocationInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listExternalLocations"></a>
# **listExternalLocations**
> ListExternalLocationsResponse listExternalLocations(max\_results, page\_token)

List external locations

    Gets an array of external locations (ExternalLocationInfo objects) from the metastore. The caller must be a metastore admin, the owner of the external location, or a user with some privilege on the external location. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **max\_results** | **Integer** | optional | Maximum number of external locations to return. If not set, all external locations are returned.  | |
| **page\_token** | **String** | optional | Opaque pagination token to go to the next page. | |

### Return type

[**ListExternalLocationsResponse**](../Models/ListExternalLocationsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateExternalLocation"></a>
# **updateExternalLocation**
> ExternalLocationInfo updateExternalLocation(name, UpdateExternalLocation)

Update an external location

    Updates an external location in the metastore. The caller must be the owner of the external location or a metastore admin. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of the external location. | |
| **UpdateExternalLocation** | [**UpdateExternalLocation**](../Models/UpdateExternalLocation.md) | required |  |  |

### Return type

[**ExternalLocationInfo**](../Models/ExternalLocationInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

