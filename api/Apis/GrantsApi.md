# GrantsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**get**](GrantsApi.md#get) | **GET** /permissions/{securable_type}/{full_name} | Get permissions |
| [**update**](GrantsApi.md#update) | **PATCH** /permissions/{securable_type}/{full_name} | Update a permission |


<a name="get"></a>
# **get**
> PermissionsList get(securable\_type, full\_name, principal)

Get permissions

    Gets the permissions for a securable. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **securable\_type** | [**SecurableType**](../Models/.md) | required | Type of securable. | [enum: metastore, catalog, schema, table, function, volume, registered_model, external_location, credential] |
| **full\_name** | **String** | required | Full name of securable. | |
| **principal** | **String** | optional | If provided, only the permissions for the specified principal (user or group) are returned.  | |

### Return type

[**PermissionsList**](../Models/PermissionsList.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="update"></a>
# **update**
> PermissionsList update(securable\_type, full\_name, UpdatePermissions)

Update a permission

    Updates the permissions for a securable. 

### Parameters

| Name | Type | Required | Description | Notes |
|------------- | ------------- | ------------- | ------------- | -------------|
| **securable\_type** | [**SecurableType**](../Models/.md) | required | Type of securable. | [enum: metastore, catalog, schema, table, function, volume, registered_model, external_location, credential] |
| **full\_name** | **String** | required | Full name of securable. | |
| **UpdatePermissions** | [**UpdatePermissions**](../Models/UpdatePermissions.md) | optional |  |  |

### Return type

[**PermissionsList**](../Models/PermissionsList.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

