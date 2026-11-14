# CredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createCredential**](CredentialsApi.md#createCredential) | **POST** /credentials | Create a credential |
| [**deleteCredential**](CredentialsApi.md#deleteCredential) | **DELETE** /credentials/{name} | Delete a credential |
| [**getCredential**](CredentialsApi.md#getCredential) | **GET** /credentials/{name} | Get a credential |
| [**listCredentials**](CredentialsApi.md#listCredentials) | **GET** /credentials | List credentials |
| [**updateCredential**](CredentialsApi.md#updateCredential) | **PATCH** /credentials/{name} | Update a credential |


<a name="createCredential"></a>
# **createCredential**
> CredentialInfo createCredential(CreateCredentialRequest)

Create a credential

    Creates a new credential. The type of credential to be created is determined by the **purpose** field. Set exactly one of &#x60;aws_iam_role&#x60; and &#x60;s3_compatible&#x60;; otherwise the request fails with &#x60;INVALID_ARGUMENT&#x60;.  EXPERIMENTAL: &#x60;s3_compatible&#x60; is available only when the server property &#x60;server.s3-compatible-storage.enabled&#x60; is &#x60;true&#x60;. Otherwise a create or update request that sets &#x60;s3_compatible&#x60;, and vending for a path under one, fail with &#x60;INVALID_ARGUMENT&#x60;; other updates, reads, lists and deletes of existing ones still work. An &#x60;s3_compatible&#x60; credential holds an access key pair of S3-compatible storage and the storage&#39;s S3 endpoint. &#x60;s3_endpoint&#x60; and &#x60;assume_role.sts_endpoint&#x60; are http or https URLs with a host, and without user info, a query or a fragment; other values fail with &#x60;INVALID_ARGUMENT&#x60;. With &#x60;assume_role&#x60;, the key pair signs AssumeRole on the storage&#39;s STS, and Unity Catalog vends short-lived credentials scoped to the requested path and privilege. Without it, the server property &#x60;server.s3-compatible-storage.static-key-vending&#x60; decides what clients receive:  * &#x60;endpoint-only&#x60; (default): the S3 endpoint and region, but no keys; engines bring their   own. * &#x60;static-key&#x60;: the key pair itself. It is not scoped to the requested path or privilege,   and changing Unity Catalog permissions does not revoke a key pair already vended.  Vends with keys (&#x60;assume_role&#x60; sessions and &#x60;static-key&#x60; key pairs) suit only servers whose clients all read &#x60;s3_endpoint&#x60;: a client that does not would send the keys to AWS S3. Without &#x60;assume_role&#x60;, in both modes the vended &#x60;expiration_time&#x60;, an hour ahead, is when a client may fetch credentials again, and the Iceberg REST table config of a &#x60;static-key&#x60; vend carries no expiration; an engine that does not fetch a vend without keys again, such as the UC Hadoop connector, picks up a changed endpoint on its first table load after &#x60;expiration_time&#x60;. The external locations of an &#x60;s3_compatible&#x60; credential must be &#x60;s3://&#x60; paths.

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

Delete a credential

    Deletes a credential from the metastore. The caller must be a metastore admin, the credential owner, or have MANAGE on the credential.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| Name of the credential. | [default to null] |
| **force** | **Boolean**| Force deletion even if there are dependent external locations. The dependent external locations are kept, and they have no credential until they are updated with a new credential_name. | [optional] [default to null] |

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

Get a credential

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

List credentials

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

Update a credential

    Updates a credential. The caller must be a metastore admin, the credential owner, or have MANAGE on the credential. Set at most one of &#x60;aws_iam_role&#x60; and &#x60;s3_compatible&#x60;; setting both fails with &#x60;INVALID_ARGUMENT&#x60;. Either one replaces the stored credential as a whole, with no field-level merge, and can change its type: an &#x60;s3_compatible&#x60; update carries &#x60;secret_access_key&#x60; again, and leaving out &#x60;assume_role&#x60; removes it. Updates without either, such as of the name or comment, keep the stored credential.  EXPERIMENTAL: whoever may update a credential is trusted with where the data under it lives. Changing &#x60;s3_endpoint&#x60; of an &#x60;s3_compatible&#x60; credential moves data access for every path under its external locations, including tables and volumes other principals own, to the new host; changing &#x60;sts_endpoint&#x60; moves where their credentials are issued. The server itself connects to both endpoints. An &#x60;s3_compatible&#x60; credential has only &#x60;s3://&#x60; external locations: setting &#x60;s3_compatible&#x60; while a location outside &#x60;s3://&#x60; uses the credential, or creating or updating such a location to use it, fails with &#x60;INVALID_ARGUMENT&#x60;.

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

