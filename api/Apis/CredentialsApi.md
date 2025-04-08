# CredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createCredential**](CredentialsApi.md#createCredential) | **POST** /credentials | Create a Credential. |
| [**deleteCredential**](CredentialsApi.md#deleteCredential) | **DELETE** /credentials/{name} | Delete a Credential. |
| [**getCredential**](CredentialsApi.md#getCredential) | **GET** /credentials/{name} | Get a Credential. |
| [**listCredentials**](CredentialsApi.md#listCredentials) | **GET** /credentials | List Credentials. |
| [**updateCredential**](CredentialsApi.md#updateCredential) | **PATCH** /credentials/{name} | Update a Credential. |


<a name="createCredential"></a>
# **createCredential**
> CredentialInfo createCredential(CreateCredentialRequest)

Create a Credential.

    Creates a new credential. The type of credential to be created is determined by the **purpose** field. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateCredentialRequest** | [**CreateCredentialRequest**](../Models/CreateCredentialRequest.md)|  | [optional] |

### Return type

[**CredentialInfo**](../Models/CredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteCredential"></a>
# **deleteCredential**
> oas_any_type_not_mapped deleteCredential(name, force)

Delete a Credential.

    Deletes a credential from the metastore. The caller must be a metastore admin or the owner of the credential. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the credential. | [default to null] |
| **force** | **Boolean**| Force deletion even if there are dependent external locations. | [optional] [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getCredential"></a>
# **getCredential**
> CredentialInfo getCredential(name)

Get a Credential.

    Gets a credential from the metastore. The caller must be a metastore admin, the owner of the credential, or have some permission on the credential. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the credential. | [default to null] |

### Return type

[**CredentialInfo**](../Models/CredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listCredentials"></a>
# **listCredentials**
> ListCredentialsResponse listCredentials(max\_results, page\_token, purpose)

List Credentials.

    Gets an array of credentials (as __CredentialInfo__ objects). The array is limited to only those credentials the caller has permission to access. If the caller is a metastore admin, retrieval of credentials is unrestricted. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **max\_results** | **Integer**| Maximum number of credentials to return.   - If not set, the default max page size is used.   - When set to a value greater than 0, the page length is the minimum of     this value and a server-configured value.   - When set to 0, the page length is set to a server-configured value     (recommended).   - When set to a value less than 0, an invalid parameter error is     returned.  | [optional] [default to null] |
| **page\_token** | **String**| Opaque pagination token to go to next page based on previous query. | [optional] [default to null] |
| **purpose** | [**CredentialPurpose**](../Models/.md)| Return only credentials for the specified purpose. | [optional] [default to null] [enum: STORAGE] |

### Return type

[**ListCredentialsResponse**](../Models/ListCredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateCredential"></a>
# **updateCredential**
> CredentialInfo updateCredential(name, UpdateCredentialRequest)

Update a Credential.

    Updates a credential. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the credential. | [default to null] |
| **UpdateCredentialRequest** | [**UpdateCredentialRequest**](../Models/UpdateCredentialRequest.md)|  | [optional] |

### Return type

[**CredentialInfo**](../Models/CredentialInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

