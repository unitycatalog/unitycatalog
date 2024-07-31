# CreateVolumeRequestContent


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalog_name** | **str** | The name of the catalog where the schema and the volume are | 
**schema_name** | **str** | The name of the schema where the volume is | 
**name** | **str** | The name of the volume | 
**volume_type** | [**VolumeType**](VolumeType.md) |  | 
**comment** | **str** | The comment attached to the volume | [optional] 
**storage_location** | **str** | The storage location of the volume | 

## Example

```python
from unitycatalog.models.create_volume_request_content import CreateVolumeRequestContent

# TODO update the JSON string below
json = "{}"
# create an instance of CreateVolumeRequestContent from a JSON string
create_volume_request_content_instance = CreateVolumeRequestContent.from_json(json)
# print the JSON string representation of the object
print(CreateVolumeRequestContent.to_json())

# convert the object into a dict
create_volume_request_content_dict = create_volume_request_content_instance.to_dict()
# create an instance of CreateVolumeRequestContent from a dict
create_volume_request_content_from_dict = CreateVolumeRequestContent.from_dict(create_volume_request_content_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


