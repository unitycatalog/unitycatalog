# unitycatalog.TablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_table**](TablesApi.md#create_table) | **POST** /tables | Create a table. WARNING: This API is experimental and will change in future versions. 
[**delete_table**](TablesApi.md#delete_table) | **DELETE** /tables/{full_name} | Delete a table
[**get_table**](TablesApi.md#get_table) | **GET** /tables/{full_name} | Get a table
[**list_tables**](TablesApi.md#list_tables) | **GET** /tables | List tables


# **create_table**
> TableInfo create_table(create_table=create_table)

Create a table. WARNING: This API is experimental and will change in future versions. 

Creates a new table instance. WARNING: This API is experimental and will change in future versions. 

### Example


```python
import unitycatalog
from unitycatalog.models.create_table import CreateTable
from unitycatalog.models.table_info import TableInfo
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
    api_instance = unitycatalog.TablesApi(api_client)
    create_table = unitycatalog.CreateTable() # CreateTable |  (optional)

    try:
        # Create a table. WARNING: This API is experimental and will change in future versions. 
        api_response = api_instance.create_table(create_table=create_table)
        print("The response of TablesApi->create_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TablesApi->create_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_table** | [**CreateTable**](CreateTable.md)|  | [optional] 

### Return type

[**TableInfo**](TableInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The new table was successfully created. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_table**
> object delete_table(full_name)

Delete a table

Deletes a table from the specified parent catalog and schema. 

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
    api_instance = unitycatalog.TablesApi(api_client)
    full_name = 'full_name_example' # str | Full name of the table.

    try:
        # Delete a table
        api_response = api_instance.delete_table(full_name)
        print("The response of TablesApi->delete_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TablesApi->delete_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **full_name** | **str**| Full name of the table. | 

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
**200** | The table was successfully deleted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_table**
> TableInfo get_table(full_name)

Get a table

Gets a table for a specific catalog and schema. 

### Example


```python
import unitycatalog
from unitycatalog.models.table_info import TableInfo
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
    api_instance = unitycatalog.TablesApi(api_client)
    full_name = 'full_name_example' # str | Full name of the table.

    try:
        # Get a table
        api_response = api_instance.get_table(full_name)
        print("The response of TablesApi->get_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TablesApi->get_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **full_name** | **str**| Full name of the table. | 

### Return type

[**TableInfo**](TableInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The table was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_tables**
> ListTablesResponse list_tables(catalog_name, schema_name, max_results=max_results, page_token=page_token)

List tables

Gets the list of all available tables under the parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Example


```python
import unitycatalog
from unitycatalog.models.list_tables_response import ListTablesResponse
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
    api_instance = unitycatalog.TablesApi(api_client)
    catalog_name = 'catalog_name_example' # str | Name of parent catalog for tables of interest.
    schema_name = 'schema_name_example' # str | Parent schema of tables.
    max_results = 56 # int | Maximum number of tables to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  (optional)
    page_token = 'page_token_example' # str | Opaque token to send for the next page of results (pagination). (optional)

    try:
        # List tables
        api_response = api_instance.list_tables(catalog_name, schema_name, max_results=max_results, page_token=page_token)
        print("The response of TablesApi->list_tables:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TablesApi->list_tables: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| Name of parent catalog for tables of interest. | 
 **schema_name** | **str**| Parent schema of tables. | 
 **max_results** | **int**| Maximum number of tables to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] 
 **page_token** | **str**| Opaque token to send for the next page of results (pagination). | [optional] 

### Return type

[**ListTablesResponse**](ListTablesResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The tables list was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

