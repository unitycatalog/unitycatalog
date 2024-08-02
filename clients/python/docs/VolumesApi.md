# unitycatalog.VolumesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_volume**](VolumesApi.md#create_volume) | **POST** /volumes | Create a Volume
[**delete_volume**](VolumesApi.md#delete_volume) | **DELETE** /volumes/{name} | Delete a Volume
[**get_volume**](VolumesApi.md#get_volume) | **GET** /volumes/{name} | Get a Volume
[**list_volumes**](VolumesApi.md#list_volumes) | **GET** /volumes | List Volumes
[**update_volume**](VolumesApi.md#update_volume) | **PATCH** /volumes/{name} | Update a Volume


# **create_volume**
> VolumeInfo create_volume(create_volume_request_content)

Create a Volume

Creates a new volume. 

### Example


```python
import unitycatalog
from unitycatalog.models.create_volume_request_content import CreateVolumeRequestContent
from unitycatalog.models.volume_info import VolumeInfo
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
    api_instance = unitycatalog.VolumesApi(api_client)
    create_volume_request_content = {"catalog_name":"main","schema_name":"default","name":"my_volume","volume_type":"EXTERNAL","comment":"This is my first volume"} # CreateVolumeRequestContent | 

    try:
        # Create a Volume
        api_response = api_instance.create_volume(create_volume_request_content)
        print("The response of VolumesApi->create_volume:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling VolumesApi->create_volume: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_volume_request_content** | [**CreateVolumeRequestContent**](CreateVolumeRequestContent.md)|  | 

### Return type

[**VolumeInfo**](VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully created the volume |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_volume**
> object delete_volume(name)

Delete a Volume

Deletes a volume from the specified parent catalog and schema. 

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
    api_instance = unitycatalog.VolumesApi(api_client)
    name = 'main.default.my_volume' # str | The three-level (fully qualified) name of the volume

    try:
        # Delete a Volume
        api_response = api_instance.delete_volume(name)
        print("The response of VolumesApi->delete_volume:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling VolumesApi->delete_volume: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The three-level (fully qualified) name of the volume | 

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
**200** | Successfully deleted the volume |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_volume**
> VolumeInfo get_volume(name)

Get a Volume

Gets a volume for a specific catalog and schema. 

### Example


```python
import unitycatalog
from unitycatalog.models.volume_info import VolumeInfo
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
    api_instance = unitycatalog.VolumesApi(api_client)
    name = 'main.default.my_volume' # str | The three-level (fully qualified) name of the volume

    try:
        # Get a Volume
        api_response = api_instance.get_volume(name)
        print("The response of VolumesApi->get_volume:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling VolumesApi->get_volume: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The three-level (fully qualified) name of the volume | 

### Return type

[**VolumeInfo**](VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully retrieved the properties of the volume |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_volumes**
> ListVolumesResponseContent list_volumes(catalog_name, schema_name, max_results=max_results, page_token=page_token)

List Volumes

Gets an array of available volumes under the parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Example


```python
import unitycatalog
from unitycatalog.models.list_volumes_response_content import ListVolumesResponseContent
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
    api_instance = unitycatalog.VolumesApi(api_client)
    catalog_name = 'main' # str | The identifier of the catalog
    schema_name = 'default' # str | The identifier of the schema
    max_results = 56 # int | Maximum number of volumes to return (page length).  If not set, the page length is set to a server configured value. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  Note: this parameter controls only the maximum number of volumes to return. The actual number of volumes returned in a page may be smaller than this value, including 0, even if there are more pages.   (optional)
    page_token = 'page_token_example' # str | Opaque token returned by a previous request. It must be included in the request to retrieve the next page of results (pagination). (optional)

    try:
        # List Volumes
        api_response = api_instance.list_volumes(catalog_name, schema_name, max_results=max_results, page_token=page_token)
        print("The response of VolumesApi->list_volumes:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling VolumesApi->list_volumes: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The identifier of the catalog | 
 **schema_name** | **str**| The identifier of the schema | 
 **max_results** | **int**| Maximum number of volumes to return (page length).  If not set, the page length is set to a server configured value. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  Note: this parameter controls only the maximum number of volumes to return. The actual number of volumes returned in a page may be smaller than this value, including 0, even if there are more pages.   | [optional] 
 **page_token** | **str**| Opaque token returned by a previous request. It must be included in the request to retrieve the next page of results (pagination). | [optional] 

### Return type

[**ListVolumesResponseContent**](ListVolumesResponseContent.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The volume list was successfully retrieved |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_volume**
> VolumeInfo update_volume(name, update_volume_request_content=update_volume_request_content)

Update a Volume

Updates the specified volume under the specified parent catalog and schema.  Currently only the name or the comment of the volume could be updated. 

### Example


```python
import unitycatalog
from unitycatalog.models.update_volume_request_content import UpdateVolumeRequestContent
from unitycatalog.models.volume_info import VolumeInfo
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
    api_instance = unitycatalog.VolumesApi(api_client)
    name = 'main.default.my_volume' # str | The three-level (fully qualified) name of the volume
    update_volume_request_content = {"new_name":"my_new_volume"} # UpdateVolumeRequestContent |  (optional)

    try:
        # Update a Volume
        api_response = api_instance.update_volume(name, update_volume_request_content=update_volume_request_content)
        print("The response of VolumesApi->update_volume:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling VolumesApi->update_volume: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**| The three-level (fully qualified) name of the volume | 
 **update_volume_request_content** | [**UpdateVolumeRequestContent**](UpdateVolumeRequestContent.md)|  | [optional] 

### Return type

[**VolumeInfo**](VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successfully updated the properties of the volume |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

