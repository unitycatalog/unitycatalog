# FunctionsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createFunction**](FunctionsApi.md#createFunction) | **POST** /functions | Create a function. WARNING: This API is experimental and will change in future versions.  |
| [**deleteFunction**](FunctionsApi.md#deleteFunction) | **DELETE** /functions/{name} | Delete a function |
| [**getFunction**](FunctionsApi.md#getFunction) | **GET** /functions/{name} | Get a function |
| [**listFunctions**](FunctionsApi.md#listFunctions) | **GET** /functions | List functions |


<a name="createFunction"></a>
# **createFunction**
> FunctionInfo createFunction(CreateFunctionRequest)

Create a function. WARNING: This API is experimental and will change in future versions. 

    Creates a new function instance. WARNING: This API is experimental and will change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateFunctionRequest** | [**CreateFunctionRequest**](../Models/CreateFunctionRequest.md)|  | [optional] |

### Return type

[**FunctionInfo**](../Models/FunctionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteFunction"></a>
# **deleteFunction**
> oas_any_type_not_mapped deleteFunction(name)

Delete a function

    Deletes the function that matches the supplied name.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__). | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getFunction"></a>
# **getFunction**
> FunctionInfo getFunction(name)

Get a function

    Gets a function from within a parent catalog and schema.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__). | [default to null] |

### Return type

[**FunctionInfo**](../Models/FunctionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listFunctions"></a>
# **listFunctions**
> ListFunctionsResponse listFunctions(catalog\_name, schema\_name, max\_results, page\_token)

List functions

    List functions within the specified parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Name of parent catalog for functions of interest. | [default to null] |
| **schema\_name** | **String**| Parent schema of functions. | [default to null] |
| **max\_results** | **Integer**| Maximum number of functions to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque pagination token to go to next page based on previous query. | [optional] [default to null] |

### Return type

[**ListFunctionsResponse**](../Models/ListFunctionsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

