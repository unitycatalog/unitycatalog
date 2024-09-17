# ModelVersionsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createModelVersion**](ModelVersionsApi.md#createModelVersion) | **POST** /models/versions | Create a model version.  |
| [**deleteModelVersion**](ModelVersionsApi.md#deleteModelVersion) | **DELETE** /models/{full_name}/versions/{version} | Delete a model version |
| [**finalizeModelVersion**](ModelVersionsApi.md#finalizeModelVersion) | **PATCH** /models/{full_name}/versions/{version}/finalize | Finalize a model version |
| [**getModelVersion**](ModelVersionsApi.md#getModelVersion) | **GET** /models/{full_name}/versions/{version} | Get a model version |
| [**listModelVersions**](ModelVersionsApi.md#listModelVersions) | **GET** /models/{full_name}/versions | List model versions of the specified registered model. |
| [**updateModelVersion**](ModelVersionsApi.md#updateModelVersion) | **PATCH** /models/{full_name}/versions/{version} | Update a model version |


<a name="createModelVersion"></a>
# **createModelVersion**
> ModelVersionInfo createModelVersion(CreateModelVersion)

Create a model version. 

    Creates a new model version instance. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateModelVersion** | [**CreateModelVersion**](../Models/CreateModelVersion.md)|  | [optional] |

### Return type

[**ModelVersionInfo**](../Models/ModelVersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteModelVersion"></a>
# **deleteModelVersion**
> oas_any_type_not_mapped deleteModelVersion(full\_name, version)

Delete a model version

    Deletes the specified model version. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **version** | **Long**| Version number of the model version. | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="finalizeModelVersion"></a>
# **finalizeModelVersion**
> ModelVersionInfo finalizeModelVersion(full\_name, version, FinalizeModelVersion)

Finalize a model version

    Finalizes the status of the specified model version. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **version** | **Long**| Version number of the model version. | [default to null] |
| **FinalizeModelVersion** | [**FinalizeModelVersion**](../Models/FinalizeModelVersion.md)|  | [optional] |

### Return type

[**ModelVersionInfo**](../Models/ModelVersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="getModelVersion"></a>
# **getModelVersion**
> ModelVersionInfo getModelVersion(full\_name, version)

Get a model version

    Gets a specific model version for a specific model. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **version** | **Long**| Version number of the model version. | [default to null] |

### Return type

[**ModelVersionInfo**](../Models/ModelVersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listModelVersions"></a>
# **listModelVersions**
> ListModelVersionsResponse listModelVersions(full\_name, max\_results, page\_token)

List model versions of the specified registered model.

    Gets the paginated list of all available model versions under the specified registered model. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the registered model. | [default to null] |
| **max\_results** | **Integer**| Maximum number of model versions to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque token to send for the next page of results (pagination). | [optional] [default to null] |

### Return type

[**ListModelVersionsResponse**](../Models/ListModelVersionsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateModelVersion"></a>
# **updateModelVersion**
> ModelVersionInfo updateModelVersion(full\_name, version, UpdateModelVersion)

Update a model version

    Updates the specified model version. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **version** | **Long**| Version number of the model version. | [default to null] |
| **UpdateModelVersion** | [**UpdateModelVersion**](../Models/UpdateModelVersion.md)|  | [optional] |

### Return type

[**ModelVersionInfo**](../Models/ModelVersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

