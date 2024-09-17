# TemporaryCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**generateTemporaryModelVersionCredentials**](TemporaryCredentialsApi.md#generateTemporaryModelVersionCredentials) | **POST** /temporary-model-version-credentials | Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location. |
| [**generateTemporaryPathCredentials**](TemporaryCredentialsApi.md#generateTemporaryPathCredentials) | **POST** /temporary-path-credentials | Generate temporary path credentials. |
| [**generateTemporaryTableCredentials**](TemporaryCredentialsApi.md#generateTemporaryTableCredentials) | **POST** /temporary-table-credentials | Generate temporary table credentials. |
| [**generateTemporaryVolumeCredentials**](TemporaryCredentialsApi.md#generateTemporaryVolumeCredentials) | **POST** /temporary-volume-credentials | Generate temporary volume credentials. |


<a name="generateTemporaryModelVersionCredentials"></a>
# **generateTemporaryModelVersionCredentials**
> TemporaryCredentials generateTemporaryModelVersionCredentials(GenerateTemporaryModelVersionCredential)

Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GenerateTemporaryModelVersionCredential** | [**GenerateTemporaryModelVersionCredential**](../Models/GenerateTemporaryModelVersionCredential.md)|  | [optional] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="generateTemporaryPathCredentials"></a>
# **generateTemporaryPathCredentials**
> TemporaryCredentials generateTemporaryPathCredentials(GenerateTemporaryPathCredential)

Generate temporary path credentials.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GenerateTemporaryPathCredential** | [**GenerateTemporaryPathCredential**](../Models/GenerateTemporaryPathCredential.md)|  | [optional] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="generateTemporaryTableCredentials"></a>
# **generateTemporaryTableCredentials**
> TemporaryCredentials generateTemporaryTableCredentials(GenerateTemporaryTableCredential)

Generate temporary table credentials.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GenerateTemporaryTableCredential** | [**GenerateTemporaryTableCredential**](../Models/GenerateTemporaryTableCredential.md)|  | [optional] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="generateTemporaryVolumeCredentials"></a>
# **generateTemporaryVolumeCredentials**
> TemporaryCredentials generateTemporaryVolumeCredentials(GenerateTemporaryVolumeCredential)

Generate temporary volume credentials.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GenerateTemporaryVolumeCredential** | [**GenerateTemporaryVolumeCredential**](../Models/GenerateTemporaryVolumeCredential.md)|  | [optional] |

### Return type

[**TemporaryCredentials**](../Models/TemporaryCredentials.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

