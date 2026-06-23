# ViewsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createView**](ViewsApi.md#createView) | **POST** /views | Create a view. Only regular view creation is supported. WARNING: This API is experimental and will change in future versions.  |
| [**deleteView**](ViewsApi.md#deleteView) | **DELETE** /views/{full_name} | Delete a view |
| [**getView**](ViewsApi.md#getView) | **GET** /views/{full_name} | Get a view |
| [**listViews**](ViewsApi.md#listViews) | **GET** /views | List views |


<a name="createView"></a>
# **createView**
> ViewInfo createView(CreateView)

Create a view. Only regular view creation is supported. WARNING: This API is experimental and will change in future versions. 

    Creates a new view instance. WARNING: This API is experimental and will change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateView** | [**CreateView**](../Models/CreateView.md)|  | [optional] |

### Return type

[**ViewInfo**](../Models/ViewInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteView"></a>
# **deleteView**
> oas_any_type_not_mapped deleteView(full\_name)

Delete a view

    Deletes a view from the specified parent catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the view. | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getView"></a>
# **getView**
> ViewInfo getView(full\_name)

Get a view

    Gets a view for a specific catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the view. | [default to null] |

### Return type

[**ViewInfo**](../Models/ViewInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listViews"></a>
# **listViews**
> ListViewsResponse listViews(catalog\_name, schema\_name, max\_results, page\_token)

List views

    Gets the list of all available views under the parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Name of parent catalog for views of interest. | [default to null] |
| **schema\_name** | **String**| Parent schema of views. | [default to null] |
| **max\_results** | **Integer**| Maximum number of views to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque token to send for the next page of results (pagination). | [optional] [default to null] |

### Return type

[**ListViewsResponse**](../Models/ListViewsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

