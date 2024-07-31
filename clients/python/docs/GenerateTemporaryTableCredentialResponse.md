# GenerateTemporaryTableCredentialResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**aws_temp_credentials** | [**AwsCredentials**](AwsCredentials.md) |  | [optional] 
**expiration_time** | **int** | Server time when the credential will expire, in epoch milliseconds. The API client is advised to cache the credential given this expiration time.  | [optional] 

## Example

```python
from unitycatalog.models.generate_temporary_table_credential_response import GenerateTemporaryTableCredentialResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GenerateTemporaryTableCredentialResponse from a JSON string
generate_temporary_table_credential_response_instance = GenerateTemporaryTableCredentialResponse.from_json(json)
# print the JSON string representation of the object
print(GenerateTemporaryTableCredentialResponse.to_json())

# convert the object into a dict
generate_temporary_table_credential_response_dict = generate_temporary_table_credential_response_instance.to_dict()
# create an instance of GenerateTemporaryTableCredentialResponse from a dict
generate_temporary_table_credential_response_from_dict = GenerateTemporaryTableCredentialResponse.from_dict(generate_temporary_table_credential_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


