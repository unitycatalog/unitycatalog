# unitycatalog.CatalogsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_catalog**](CatalogsApi.md#create_catalog) | **POST** /catalogs | Create a catalog
[**delete_catalog**](CatalogsApi.md#delete_catalog) | **DELETE** /catalogs/{name} | Delete a catalog
[**get_catalog**](CatalogsApi.md#get_catalog) | **GET** /catalogs/{name} | Get a catalog
[**list_catalogs**](CatalogsApi.md#list_catalogs) | **GET** /catalogs | List catalogs
[**update_catalog**](CatalogsApi.md#update_catalog) | **PATCH** /catalogs/{name} | Update a catalog


# **create_catalog**
> CatalogInfo create_catalog(create_catalog=create_catalog)

Create a catalog

Creates a new catalog instance. 

### Example


```python
import unitycatalog
from unitycatalog.models.catalog_info import CatalogInfo
from unitycatalog.models.create_catalog import CreateCatalog
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
    api_instance = unitycatalog.CatalogsApi(api_client)
    create_catalog = unitycatalog.CreateCatalog() # CreateCatalog |  (optional)

    try:
        # Create a catalog
        api_response = await api_instance.create_catalog(create_catalog=create_catalog)
        print("The response of CatalogsApi->create_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogsApi->create_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_catalog** | [**CreateCatalog**](CreateCatalog.md)|  | [optional] 

### Return type

[**CatalogInfo**](CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The new catalog was successfully created. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_catalog**
> object delete_catalog(name, force=force)

Delete a catalog

Deletes the catalog that matches the supplied name. 

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
    api_instance = unitycatalog.CatalogsApi(api_client)
    name = 'name_example' # str | The name of the catalog.
    force = True # bool | Force deletion even if the catalog is not empty. (optional)

    try:
        # Delete a catalog
        api_response = await api_instance.delete_catalog(name, force=force)
        print("The response of CatalogsApi->delete_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogsApi->delete_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The name of the catalog. | 
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
**200** | The catalog was successfully deleted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_catalog**
> CatalogInfo get_catalog(name)

Get a catalog

Gets the specified catalog. 

### Example


```python
import unitycatalog
from unitycatalog.models.catalog_info import CatalogInfo
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
    api_instance = unitycatalog.CatalogsApi(api_client)
    name = 'name_example' # str | The name of the catalog.

    try:
        # Get a catalog
        api_response = await api_instance.get_catalog(name)
        print("The response of CatalogsApi->get_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogsApi->get_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The name of the catalog. | 

### Return type

[**CatalogInfo**](CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The catalog was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_catalogs**
> ListCatalogsResponse list_catalogs(page_token=page_token, max_results=max_results)

List catalogs

Lists the available catalogs. There is no guarantee of a specific ordering of the elements in the list. 

### Example


```python
import unitycatalog
from unitycatalog.models.list_catalogs_response import ListCatalogsResponse
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
    api_instance = unitycatalog.CatalogsApi(api_client)
    page_token = 'page_token_example' # str | Opaque pagination token to go to next page based on previous query.  (optional)
    max_results = 56 # int | Maximum number of catalogs to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  (optional)

    try:
        # List catalogs
        api_response = await api_instance.list_catalogs(page_token=page_token, max_results=max_results)
        print("The response of CatalogsApi->list_catalogs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogsApi->list_catalogs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **page_token** | **str**| Opaque pagination token to go to next page based on previous query.  | [optional] 
 **max_results** | **int**| Maximum number of catalogs to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] 

### Return type

[**ListCatalogsResponse**](ListCatalogsResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The catalog list was successfully retrieved. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_catalog**
> CatalogInfo update_catalog(name, update_catalog=update_catalog)

Update a catalog

Updates the catalog that matches the supplied name. 

### Example


```python
import unitycatalog
from unitycatalog.models.catalog_info import CatalogInfo
from unitycatalog.models.update_catalog import UpdateCatalog
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
    api_instance = unitycatalog.CatalogsApi(api_client)
    name = 'name_example' # str | The name of the catalog.
    update_catalog = unitycatalog.UpdateCatalog() # UpdateCatalog |  (optional)

    try:
        # Update a catalog
        api_response = await api_instance.update_catalog(name, update_catalog=update_catalog)
        print("The response of CatalogsApi->update_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogsApi->update_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The name of the catalog. | 
 **update_catalog** | [**UpdateCatalog**](UpdateCatalog.md)|  | [optional] 

### Return type

[**CatalogInfo**](CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The catalog was successfully updated. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

