# unitycatalog.TemporaryVolumeCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**generate_temporary_volume_credentials**](TemporaryVolumeCredentialsApi.md#generate_temporary_volume_credentials) | **POST** /temporary-volume-credentials | Generate temporary volume credentials.


# **generate_temporary_volume_credentials**
> GenerateTemporaryVolumeCredentialResponse generate_temporary_volume_credentials(generate_temporary_volume_credential=generate_temporary_volume_credential)

Generate temporary volume credentials.

### Example


```python
import unitycatalog
from unitycatalog.models.generate_temporary_volume_credential import GenerateTemporaryVolumeCredential
from unitycatalog.models.generate_temporary_volume_credential_response import GenerateTemporaryVolumeCredentialResponse
from unitycatalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost:8080/api/2.1/unity-catalog
# See configuration.py for a list of all supported configuration parameters.
configuration = unitycatalog.Configuration(
    host = "http://localhost:8080/api/2.1/unity-catalog"
)


# Enter a context with an instance of the API client
async with unitycatalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = unitycatalog.TemporaryVolumeCredentialsApi(api_client)
    generate_temporary_volume_credential = unitycatalog.GenerateTemporaryVolumeCredential() # GenerateTemporaryVolumeCredential |  (optional)

    try:
        # Generate temporary volume credentials.
        api_response = await api_instance.generate_temporary_volume_credentials(generate_temporary_volume_credential=generate_temporary_volume_credential)
        print("The response of TemporaryVolumeCredentialsApi->generate_temporary_volume_credentials:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TemporaryVolumeCredentialsApi->generate_temporary_volume_credentials: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **generate_temporary_volume_credential** | [**GenerateTemporaryVolumeCredential**](GenerateTemporaryVolumeCredential.md)|  | [optional] 

### Return type

[**GenerateTemporaryVolumeCredentialResponse**](GenerateTemporaryVolumeCredentialResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

