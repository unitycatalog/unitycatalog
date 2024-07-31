# unitycatalog.TemporaryTableCredentialsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

Method | HTTP request | Description
------------- | ------------- | -------------
[**generate_temporary_table_credentials**](TemporaryTableCredentialsApi.md#generate_temporary_table_credentials) | **POST** /temporary-table-credentials | Generate temporary table credentials.


# **generate_temporary_table_credentials**
> GenerateTemporaryTableCredentialResponse generate_temporary_table_credentials(generate_temporary_table_credential=generate_temporary_table_credential)

Generate temporary table credentials.

### Example


```python
import unitycatalog
from unitycatalog.models.generate_temporary_table_credential import GenerateTemporaryTableCredential
from unitycatalog.models.generate_temporary_table_credential_response import GenerateTemporaryTableCredentialResponse
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
    api_instance = unitycatalog.TemporaryTableCredentialsApi(api_client)
    generate_temporary_table_credential = unitycatalog.GenerateTemporaryTableCredential() # GenerateTemporaryTableCredential |  (optional)

    try:
        # Generate temporary table credentials.
        api_response = await api_instance.generate_temporary_table_credentials(generate_temporary_table_credential=generate_temporary_table_credential)
        print("The response of TemporaryTableCredentialsApi->generate_temporary_table_credentials:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TemporaryTableCredentialsApi->generate_temporary_table_credentials: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **generate_temporary_table_credential** | [**GenerateTemporaryTableCredential**](GenerateTemporaryTableCredential.md)|  | [optional] 

### Return type

[**GenerateTemporaryTableCredentialResponse**](GenerateTemporaryTableCredentialResponse.md)

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

