# GenerateTemporaryVolumeCredential


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**volume_id** | **str** | Volume id for which temporary credentials are generated.  Can be obtained from volumes/{full_name} (get volume info) API.  | 
**operation** | [**VolumeOperation**](VolumeOperation.md) |  | 

## Example

```python
from unitycatalog.models.generate_temporary_volume_credential import GenerateTemporaryVolumeCredential

# TODO update the JSON string below
json = "{}"
# create an instance of GenerateTemporaryVolumeCredential from a JSON string
generate_temporary_volume_credential_instance = GenerateTemporaryVolumeCredential.from_json(json)
# print the JSON string representation of the object
print(GenerateTemporaryVolumeCredential.to_json())

# convert the object into a dict
generate_temporary_volume_credential_dict = generate_temporary_volume_credential_instance.to_dict()
# create an instance of GenerateTemporaryVolumeCredential from a dict
generate_temporary_volume_credential_from_dict = GenerateTemporaryVolumeCredential.from_dict(generate_temporary_volume_credential_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


