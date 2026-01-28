# ViewsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createView**](ViewsApi.md#createView) | **POST** /views | Create a view. Only external table creation is supported. WARNING: This API is experimental and will change in future versions.  |
| [**deleteView**](ViewsApi.md#deleteView) | **DELETE** /views/{full_name} | Delete a view |
| [**getView**](ViewsApi.md#getView) | **GET** /views/{full_name} | Get a table |
| [**listViews**](ViewsApi.md#listViews) | **GET** /views | List views |


<a name="createView"></a>
# **createView**
> ViewInfo createView(CreateView)

Create a view. Only external table creation is supported. WARNING: This API is experimental and will change in future versions. 

    Creates a new view. WARNING: This API is experimental and will change in future versions. 

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

Get a table

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

    Gets a list of views for a specific catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Name of the catalog. | [default to null] |
| **schema\_name** | **String**| Name of the schema. | [default to null] |
| **max\_results** | **Integer**| Maximum number of views to return. | [optional] [default to null] |
| **page\_token** | **String**| Opaque token to retrieve the next page of results. | [optional] [default to null] |

### Return type

[**ListViewsResponse**](../Models/ListViewsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

