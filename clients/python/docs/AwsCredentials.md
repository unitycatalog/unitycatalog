# AwsCredentials


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_key_id** | **str** | The access key ID that identifies the temporary credentials. | [optional] 
**secret_access_key** | **str** | The secret access key that can be used to sign AWS API requests. | [optional] 
**session_token** | **str** | The token that users must pass to AWS API to use the temporary credentials. | [optional] 

## Example

```python
from unitycatalog.models.aws_credentials import AwsCredentials

# TODO update the JSON string below
json = "{}"
# create an instance of AwsCredentials from a JSON string
aws_credentials_instance = AwsCredentials.from_json(json)
# print the JSON string representation of the object
print(AwsCredentials.to_json())

# convert the object into a dict
aws_credentials_dict = aws_credentials_instance.to_dict()
# create an instance of AwsCredentials from a dict
aws_credentials_from_dict = AwsCredentials.from_dict(aws_credentials_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


