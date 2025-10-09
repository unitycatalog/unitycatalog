# RegisteredModelsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createRegisteredModel**](RegisteredModelsApi.md#createRegisteredModel) | **POST** /models | Create a Model |
| [**deleteRegisteredModel**](RegisteredModelsApi.md#deleteRegisteredModel) | **DELETE** /models/{full_name} | Delete a Registered Model |
| [**getRegisteredModel**](RegisteredModelsApi.md#getRegisteredModel) | **GET** /models/{full_name} | Get a Registered Model |
| [**listRegisteredModels**](RegisteredModelsApi.md#listRegisteredModels) | **GET** /models | List Models |
| [**updateRegisteredModel**](RegisteredModelsApi.md#updateRegisteredModel) | **PATCH** /models/{full_name} | Update a Registered Model |


<a name="createRegisteredModel"></a>
# **createRegisteredModel**
> RegisteredModelInfo createRegisteredModel(CreateRegisteredModel)

Create a Model

    Creates a new model instance. WARNING: This API is experimental and will change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateRegisteredModel** | [**CreateRegisteredModel**](../Models/CreateRegisteredModel.md)|  | [optional] |

### Return type

[**RegisteredModelInfo**](../Models/RegisteredModelInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteRegisteredModel"></a>
# **deleteRegisteredModel**
> oas_any_type_not_mapped deleteRegisteredModel(full\_name, force)

Delete a Registered Model

    Deletes a fully specified registered model. All versions of the model must have already been deleted. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **force** | **Boolean**| Force deletion even if the registered model still has model versions. | [optional] [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getRegisteredModel"></a>
# **getRegisteredModel**
> RegisteredModelInfo getRegisteredModel(full\_name)

Get a Registered Model

    Gets a fully specified registered model. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |

### Return type

[**RegisteredModelInfo**](../Models/RegisteredModelInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listRegisteredModels"></a>
# **listRegisteredModels**
> ListRegisteredModelsResponse listRegisteredModels(catalog\_name, schema\_name, max\_results, page\_token)

List Models

    Gets a paginated list of all available models either under the specified parent catalog and schema, or all models stored in UC. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Name of parent catalog for models of interest. | [optional] [default to null] |
| **schema\_name** | **String**| Name of parent schema for models of interest. | [optional] [default to null] |
| **max\_results** | **Integer**| Maximum number of models to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque token to send for the next page of results (pagination). | [optional] [default to null] |

### Return type

[**ListRegisteredModelsResponse**](../Models/ListRegisteredModelsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateRegisteredModel"></a>
# **updateRegisteredModel**
> RegisteredModelInfo updateRegisteredModel(full\_name, UpdateRegisteredModel)

Update a Registered Model

    Updates the specified registered model. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the model. | [default to null] |
| **UpdateRegisteredModel** | [**UpdateRegisteredModel**](../Models/UpdateRegisteredModel.md)|  | [optional] |

### Return type

[**RegisteredModelInfo**](../Models/RegisteredModelInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

