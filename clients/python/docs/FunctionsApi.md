# unitycatalog.FunctionsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_function**](FunctionsApi.md#create_function) | **POST** /functions | Create a function. WARNING: This API is experimental and will change in future versions. 
[**delete_function**](FunctionsApi.md#delete_function) | **DELETE** /functions/{name} | Delete a function
[**get_function**](FunctionsApi.md#get_function) | **GET** /functions/{name} | Get a function
[**list_functions**](FunctionsApi.md#list_functions) | **GET** /functions | List functions


# **create_function**
> FunctionInfo create_function(create_function_request=create_function_request)

Create a function. WARNING: This API is experimental and will change in future versions. 

Creates a new function instance. WARNING: This API is experimental and will change in future versions. 

### Example


```python
import unitycatalog
from unitycatalog.models.create_function_request import CreateFunctionRequest
from unitycatalog.models.function_info import FunctionInfo
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
async with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.FunctionsApi(api_client)
    create_function_request = unitycatalog.CreateFunctionRequest() # CreateFunctionRequest |  (optional)

    try:
        # Create a function. WARNING: This API is experimental and will change in future versions. 
        api_response = await api_instance.create_function(create_function_request=create_function_request)
        print("The response of FunctionsApi->create_function:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FunctionsApi->create_function: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_function_request** | [**CreateFunctionRequest**](CreateFunctionRequest.md)|  | [optional] 

### Return type

[**FunctionInfo**](FunctionInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The new function was successfully created. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_function**
> object delete_function(name)

Delete a function

Deletes the function that matches the supplied name.

### Example


```python
import unitycatalog
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
async with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.FunctionsApi(api_client)
    name = 'name_example' # str | The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__).

    try:
        # Delete a function
        api_response = await api_instance.delete_function(name)
        print("The response of FunctionsApi->delete_function:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FunctionsApi->delete_function: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__). | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The function was successfully deleted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_function**
> FunctionInfo get_function(name)

Get a function

Gets a function from within a parent catalog and schema.

### Example


```python
import unitycatalog
from unitycatalog.models.function_info import FunctionInfo
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
async with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.FunctionsApi(api_client)
    name = 'name_example' # str | The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__).

    try:
        # Get a function
        api_response = await api_instance.get_function(name)
        print("The response of FunctionsApi->get_function:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FunctionsApi->get_function: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The fully-qualified name of the function (of the form __catalog_name__.__schema_name__.__function__name__). | 

### Return type

[**FunctionInfo**](FunctionInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The function was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_functions**
> ListFunctionsResponse list_functions(catalog_name, schema_name, max_results=max_results, page_token=page_token)

List functions

List functions within the specified parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Example


```python
import unitycatalog
from unitycatalog.models.list_functions_response import ListFunctionsResponse
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
async with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.FunctionsApi(api_client)
    catalog_name = 'catalog_name_example' # str | Name of parent catalog for functions of interest.
    schema_name = 'schema_name_example' # str | Parent schema of functions.
    max_results = 56 # int | Maximum number of functions to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  (optional)
    page_token = 'page_token_example' # str | Opaque pagination token to go to next page based on previous query. (optional)

    try:
        # List functions
        api_response = await api_instance.list_functions(catalog_name, schema_name, max_results=max_results, page_token=page_token)
        print("The response of FunctionsApi->list_functions:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FunctionsApi->list_functions: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| Name of parent catalog for functions of interest. | 
 **schema_name** | **str**| Parent schema of functions. | 
 **max_results** | **int**| Maximum number of functions to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] 
 **page_token** | **str**| Opaque pagination token to go to next page based on previous query. | [optional] 

### Return type

[**ListFunctionsResponse**](ListFunctionsResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The function list was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

