# unitycatalog.SchemasApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_schema**](SchemasApi.md#create_schema) | **POST** /schemas | Create a schema
[**delete_schema**](SchemasApi.md#delete_schema) | **DELETE** /schemas/{full_name} | Delete a schema
[**get_schema**](SchemasApi.md#get_schema) | **GET** /schemas/{full_name} | Get a schema
[**list_schemas**](SchemasApi.md#list_schemas) | **GET** /schemas | List schemas
[**update_schema**](SchemasApi.md#update_schema) | **PATCH** /schemas/{full_name} | Update a schema


# **create_schema**
> SchemaInfo create_schema(create_schema=create_schema)

Create a schema

Creates a new schema in the specified catalog. 

### Example


```python
import unitycatalog
from unitycatalog.models.create_schema import CreateSchema
from unitycatalog.models.schema_info import SchemaInfo
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.SchemasApi(api_client)
    create_schema = unitycatalog.CreateSchema() # CreateSchema |  (optional)

    try:
        # Create a schema
        api_response = api_instance.create_schema(create_schema=create_schema)
        print("The response of SchemasApi->create_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemasApi->create_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_schema** | [**CreateSchema**](CreateSchema.md)|  | [optional] 

### Return type

[**SchemaInfo**](SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The new schema was successfully created. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_schema**
> object delete_schema(full_name, force=force)

Delete a schema

Deletes the specified schema from the parent catalog. 

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
with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.SchemasApi(api_client)
    full_name = 'full_name_example' # str | Full name of the schema.
    force = True # bool | Force deletion even if the catalog is not empty. (optional)

    try:
        # Delete a schema
        api_response = api_instance.delete_schema(full_name, force=force)
        print("The response of SchemasApi->delete_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemasApi->delete_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **full_name** | **str**| Full name of the schema. | 
 **force** | **bool**| Force deletion even if the catalog is not empty. | [optional] 

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
**200** | The schema was successfully deleted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema**
> SchemaInfo get_schema(full_name)

Get a schema

Gets the specified schema for a catalog. 

### Example


```python
import unitycatalog
from unitycatalog.models.schema_info import SchemaInfo
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.SchemasApi(api_client)
    full_name = 'full_name_example' # str | Full name of the schema.

    try:
        # Get a schema
        api_response = api_instance.get_schema(full_name)
        print("The response of SchemasApi->get_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemasApi->get_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **full_name** | **str**| Full name of the schema. | 

### Return type

[**SchemaInfo**](SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The schema was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_schemas**
> ListSchemasResponse list_schemas(catalog_name, max_results=max_results, page_token=page_token)

List schemas

Gets an array of schemas for a catalog. There is no guarantee of a specific ordering of the elements in the array. 

### Example


```python
import unitycatalog
from unitycatalog.models.list_schemas_response import ListSchemasResponse
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.SchemasApi(api_client)
    catalog_name = 'catalog_name_example' # str | Parent catalog for schemas of interest.
    max_results = 56 # int | Maximum number of schemas to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  (optional)
    page_token = 'page_token_example' # str | Opaque pagination token to go to next page based on previous query.  (optional)

    try:
        # List schemas
        api_response = api_instance.list_schemas(catalog_name, max_results=max_results, page_token=page_token)
        print("The response of SchemasApi->list_schemas:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemasApi->list_schemas: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| Parent catalog for schemas of interest. | 
 **max_results** | **int**| Maximum number of schemas to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] 
 **page_token** | **str**| Opaque pagination token to go to next page based on previous query.  | [optional] 

### Return type

[**ListSchemasResponse**](ListSchemasResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The schemas list was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_schema**
> SchemaInfo update_schema(full_name, update_schema=update_schema)

Update a schema

Updates the specified schema. 

### Example


```python
import unitycatalog
from unitycatalog.models.schema_info import SchemaInfo
from unitycatalog.models.update_schema import UpdateSchema
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.SchemasApi(api_client)
    full_name = 'full_name_example' # str | Full name of the schema.
    update_schema = unitycatalog.UpdateSchema() # UpdateSchema |  (optional)

    try:
        # Update a schema
        api_response = api_instance.update_schema(full_name, update_schema=update_schema)
        print("The response of SchemasApi->update_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemasApi->update_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **full_name** | **str**| Full name of the schema. | 
 **update_schema** | [**UpdateSchema**](UpdateSchema.md)|  | [optional] 

### Return type

[**SchemaInfo**](SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The schema was successfully updated. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

