# TemporaryCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**generateTemporaryModelVersionCredentials**](TemporaryCredentialsApi.md#generateTemporaryModelVersionCredentials) | **POST** /temporary-model-version-credentials | Generate Temporary Model Version Credentials |
| [**generateTemporaryPathCredentials**](TemporaryCredentialsApi.md#generateTemporaryPathCredentials) | **POST** /temporary-path-credentials | Generate Temporary Path Credentials |
| [**generateTemporaryTableCredentials**](TemporaryCredentialsApi.md#generateTemporaryTableCredentials) | **POST** /temporary-table-credentials | Generate Temporary Table Credentials |
| [**generateTemporaryVolumeCredentials**](TemporaryCredentialsApi.md#generateTemporaryVolumeCredentials) | **POST** /temporary-volume-credentials | Generate Temporary Volume Credentials |


<a name="generateTemporaryModelVersionCredentials"></a>
# **generateTemporaryModelVersionCredentials**
> TemporaryCredentials generateTemporaryModelVersionCredentials(GenerateTemporaryModelVersionCredential)

Generate Temporary Model Version Credentials

    Generates temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model version&#39;s external storage location. 

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

Generate Temporary Path Credentials

    Generates temporary credentials for accessing a storage path. 

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

Generate Temporary Table Credentials

    Generates temporary credentials for accessing table data. 

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

Generate Temporary Volume Credentials

    Generates temporary credentials for accessing volume data. 

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

