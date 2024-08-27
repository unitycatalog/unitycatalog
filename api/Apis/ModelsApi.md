# ModelsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method                                                          | HTTP request                                      | Description               |
|-----------------------------------------------------------------|---------------------------------------------------|---------------------------|
| [**createRegisteredModel**](ModelsApi.md#createRegisteredModel) | **POST** /models                                  | Create a registered model |
| [**deleteRegisteredModel**](ModelsApi.md#deleteRegisteredModel) | **DELETE** /models/{full_name}                    | Delete a registered model |
| [**getRegisteredModel**](ModelsApi.md#getRegisteredModel)       | **GET** /models/{full_name}                       | Get a registered model    |
| [**listRegisteredModels**](ModelsApi.md#listRegisteredModels)   | **GET** /models                                   | List registered models    |
| [**updateRegisteredModel**](ModelsApi.md#updateRegisteredModel) | **PATCH** /models/{full_name}                     | Update a registered model |
| [**createModelVersion**](ModelsApi.md#createModelVersion)       | **POST** /models/versions                         | Create a model version    |
| [**deleteModelVersion**](ModelsApi.md#deleteModelVersion)       | **DELETE** /models/{full_name}/versions/{version} | Delete a model version    |
| [**getModelVersion**](ModelsApi.md#getModelVersion)             | **GET** /models/{full_name}/versions/{version}    | Get a model version       |
| [**listModelVersions**](ModelsApi.md#listModelVersions)         | **GET** /models/{full_name}/versions              | List model versions       |
| [**updateModelVersion**](ModelsApi.md#updateModelVersion)       | **PATCH** /models/{full_name}/versions/{version}  | Update a model version    |


<a name="createRegisteredModel"></a>
# **createRegisteredModel**
> RegisteredModelInfo createRegisteredModel(CreateRegisteredModel)

Create a registered model

    Creates a new registered model in the specified schema. 

### Parameters

| Name                      | Type                                                            | Description  | Notes |
|---------------------------|-----------------------------------------------------------------| ------------- | -------------|
| **CreateRegisteredModel** | [**CreateRegisteredModel**](../Models/CreateRegisteredModel.md) |  | [optional] |

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

Delete a registered model

    Deletes the specified registered model from the parent schema. 

### Parameters

|Name | Type | Description                                                          | Notes |
|------------- | ------------- |----------------------------------------------------------------------| -------------|
| **full\_name** | **String**| Full name of the registered model.                                   | [default to null] |
| **force** | **Boolean**| Force deletion even if the registered model contains model versions. | [optional] [default to null] |

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

Get a registered model

    Gets the specified registered model for a schema. 

### Parameters

|Name | Type | Description                        | Notes |
|------------- | ------------- |------------------------------------| -------------|
| **full\_name** | **String**| Full name of the registered model. | [default to null] |

### Return type

[**RegisteredModelInfo**](../Models/RegisteredModelInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listRegisteredModels"></a>
# **listRegisteredModels**
> ListRegisteredModelsResponse listRegisteredModels(catalog\_name, max\_results, page\_token)

List registered models

    Gets an array of registered models for a schema. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

| Name              | Type | Description                                                                                                                                                                                                                                                                                                       | Notes |
|-------------------| ------------- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| -------------|
| **catalog\_name** | **String**| Parent catalog for registered models of interest.                                                                                                                                                                                                                                                                 | [default to null] |
| **schema\_name**  | **String**| Parent schema for registered models of interest.                                                                                                                                                                                                                                                                  | [default to null] |
| **max\_results**  | **Integer**| Maximum number of registered models to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned; | [optional] [default to null] |
| **page\_token**   | **String**| Opaque pagination token to go to next page based on previous query.                                                                                                                                                                                                                                               | [optional] [default to null] |

### Return type

[**ListRegisteredModelsResponse**](../Models/ListRegisteredModelsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateRegisteredModel"></a>
# **updateRegisteredModel**
> RegisteredModelInfo updateRegisteredModel(full\_name, new\_name, comment)

Update a registered model

    Updates the specified registered model. 

### Parameters

| Name           | Type       | Description                          | Notes             |
|----------------|------------|--------------------------------------|-------------------|
| **full\_name** | **String** | Full name of the registered model.   | [default to null] |
| **new\_name**  | **String** | New name of the registered model.    | [optional]        |
| **comment**    | **String** | New comment of the registered model. | [optional]        |

### Return type

[**RegisteredModelInfo**](../Models/RegisteredModelInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


<a name="createModelVersion"></a>
# **createModelVersion**
> ModelVersionInfo createModelVersion(CreateModelVersion)

Create a model version

    Creates a new model version under the specified registered model. 

### Parameters

| Name                      | Type                                                            | Description  | Notes |
|---------------------------|-----------------------------------------------------------------| ------------- | -------------|
| **CreateModelVersion** | [**CreateModelVersion**](../Models/CreateModelVersion.md) |  | [optional] |

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

Delete a registered model

    Deletes the specified registered model from the parent schema. 

### Parameters

| Name           | Type       | Description                         | Notes |
|----------------|------------|-------------------------------------| -------------|
| **full\_name** | **String** | Full name of the registered model.  | [default to null] |
| **version**    | **Long**   | Version number of the model version | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getModelVersion"></a>
# **getModelVersion**
> ModelVersionInfo getModelVersion(full\_name, version)

Get a registered model

    Gets the specified registered model for a schema. 

### Parameters

|Name | Type       | Description                        | Notes |
|------------- |------------|------------------------------------| -------------|
| **full\_name** | **String** | Full name of the registered model. | [default to null] |
| **version**    | **Long**   | Version number of the model version | [default to null] |

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

List ModelVersions

    Gets an array of model versions for a registered model. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

| Name             | Type | Description                                                                                                                                                                                                                                                                                                    | Notes |
|------------------| ------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| -------------|
| **full\_name**   | **String**| Full name of the registered models of interest.                                                                                                                                                                                                                                                                | [default to null] |
| **max\_results** | **Integer**| Maximum number of model versions to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned; | [optional] [default to null] |
| **page\_token**  | **String**| Opaque pagination token to go to next page based on previous query.                                                                                                                                                                                                                                            | [optional] [default to null] |

### Return type

[**ListModelVersionsResponse**](../Models/ListModelVersionsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateModelVersion"></a>
# **updateModelVersion**
> ModelVersionInfo updateModelVersion(full\_name, version, comment)

Update a model version

    Updates the specified registered model. 

### Parameters

| Name           | Type       | Description                          | Notes             |
|----------------|------------|--------------------------------------|-------------------|
| **full\_name** | **String** | Full name of the registered model.   | [default to null] |
| **version**    | **Long**   | Version number of the model version | [default to null] |
| **comment**    | **String** | New comment of the registered model. | [optional]        |

### Return type

[**ModelVersionInfo**](../Models/ModelVersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

