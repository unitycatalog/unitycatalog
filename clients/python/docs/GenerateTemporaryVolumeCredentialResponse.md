# GenerateTemporaryVolumeCredentialResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**aws_temp_credentials** | [**AwsCredentials**](AwsCredentials.md) |  | [optional] 
**expiration_time** | **int** | Server time when the credential will expire, in epoch milliseconds. The API client is advised to cache the credential given this expiration time.  | [optional] 

## Example

```python
from unitycatalog.models.generate_temporary_volume_credential_response import GenerateTemporaryVolumeCredentialResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GenerateTemporaryVolumeCredentialResponse from a JSON string
generate_temporary_volume_credential_response_instance = GenerateTemporaryVolumeCredentialResponse.from_json(json)
# print the JSON string representation of the object
print(GenerateTemporaryVolumeCredentialResponse.to_json())

# convert the object into a dict
generate_temporary_volume_credential_response_dict = generate_temporary_volume_credential_response_instance.to_dict()
# create an instance of GenerateTemporaryVolumeCredentialResponse from a dict
generate_temporary_volume_credential_response_from_dict = GenerateTemporaryVolumeCredentialResponse.from_dict(generate_temporary_volume_credential_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


