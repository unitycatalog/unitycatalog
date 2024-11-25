# StorageCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStorageCredential**](StorageCredentialsApi.md#createStorageCredential) | **POST** /storage-credentials | Create a storage credential |
| [**deleteStorageCredential**](StorageCredentialsApi.md#deleteStorageCredential) | **DELETE** /storage-credentials/{name} | Delete a credential |
| [**getStorageCredential**](StorageCredentialsApi.md#getStorageCredential) | **GET** /storage-credentials/{name} | Get a credential |
| [**listStorageCredentials**](StorageCredentialsApi.md#listStorageCredentials) | **GET** /storage-credentials | List credentials |
| [**updateStorageCredential**](StorageCredentialsApi.md#updateStorageCredential) | **PATCH** /storage-credentials/{name} | Update a credential |


<a name="createStorageCredential"></a>
# **createStorageCredential**
> StorageCredentialInfo createStorageCredential(CreateStorageCredential)

Create a storage credential

    Creates a new storage credential.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateStorageCredential** | [**CreateStorageCredential**](../Models/CreateStorageCredential.md)|  | [optional] |

### Return type

[**StorageCredentialInfo**](../Models/StorageCredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteStorageCredential"></a>
# **deleteStorageCredential**
> oas_any_type_not_mapped deleteStorageCredential(name, force)

Delete a credential

    Deletes a storage credential from the metastore. The caller must be an owner of the storage credential.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the storage credential. | [default to null] |
| **force** | **Boolean**| Force deletion even if there are dependent external locations or external tables. | [optional] [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getStorageCredential"></a>
# **getStorageCredential**
> StorageCredentialInfo getStorageCredential(name)

Get a credential

    Gets a storage credential from the metastore. The caller must be a metastore admin, the owner of the storage credential, or have some permission on the storage credential. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the storage credential. | [default to null] |

### Return type

[**StorageCredentialInfo**](../Models/StorageCredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listStorageCredentials"></a>
# **listStorageCredentials**
> ListStorageCredentialsResponse listStorageCredentials(max\_results, page\_token)

List credentials

    Gets an array of storage credentials (as __StorageCredentialInfo__ objects). The array is limited to only those storage credentials the caller has permission to access. If the caller is a metastore admin, retrieval of credentials is unrestricted. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **max\_results** | **Integer**| Maximum number of storage credentials to return. If not set, all the storage credentials are returned (not recommended). - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value (recommended); - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque pagination token to go to next page based on previous query. | [optional] [default to null] |

### Return type

[**ListStorageCredentialsResponse**](../Models/ListStorageCredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateStorageCredential"></a>
# **updateStorageCredential**
> StorageCredentialInfo updateStorageCredential(name, UpdateStorageCredential)

Update a credential

    Updates a storage credential on the metastore.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the storage credential. | [default to null] |
| **UpdateStorageCredential** | [**UpdateStorageCredential**](../Models/UpdateStorageCredential.md)|  | [optional] |

### Return type

[**StorageCredentialInfo**](../Models/StorageCredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

