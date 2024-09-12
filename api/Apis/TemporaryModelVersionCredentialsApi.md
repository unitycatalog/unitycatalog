# TemporaryModelVersionCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**generateTemporaryModelVersionCredentials**](TemporaryModelVersionCredentialsApi.md#generateTemporaryModelVersionCredentials) | **POST** /temporary-model-version-credentials | Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location. |


<a name="generateTemporaryModelVersionCredentials"></a>
# **generateTemporaryModelVersionCredentials**
> GenerateTemporaryModelVersionCredentialsResponse generateTemporaryModelVersionCredentials(GenerateTemporaryModelVersionCredentials)

Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GenerateTemporaryModelVersionCredentials** | [**GenerateTemporaryModelVersionCredentials**](../Models/GenerateTemporaryModelVersionCredentials.md)|  | [optional] |

### Return type

[**GenerateTemporaryModelVersionCredentialsResponse**](../Models/GenerateTemporaryModelVersionCredentialsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

