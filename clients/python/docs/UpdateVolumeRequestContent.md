# UpdateVolumeRequestContent


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**comment** | **str** | The comment attached to the volume | [optional] 
**new_name** | **str** | New name for the volume. | [optional] 

## Example

```python
from unitycatalog.models.update_volume_request_content import UpdateVolumeRequestContent

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateVolumeRequestContent from a JSON string
update_volume_request_content_instance = UpdateVolumeRequestContent.from_json(json)
# print the JSON string representation of the object
print(UpdateVolumeRequestContent.to_json())

# convert the object into a dict
update_volume_request_content_dict = update_volume_request_content_instance.to_dict()
# create an instance of UpdateVolumeRequestContent from a dict
update_volume_request_content_from_dict = UpdateVolumeRequestContent.from_dict(update_volume_request_content_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


