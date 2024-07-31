# VolumeInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalog_name** | **str** | The name of the catalog where the schema and the volume are | [optional] 
**schema_name** | **str** | The name of the schema where the volume is | [optional] 
**name** | **str** | The name of the volume | [optional] 
**comment** | **str** | The comment attached to the volume | [optional] 
**created_at** | **int** | Time at which this volume was created, in epoch milliseconds. | [optional] 
**updated_at** | **int** | Time at which this volume was last modified, in epoch milliseconds. | [optional] 
**volume_id** | **str** | Unique identifier for the volume | [optional] 
**volume_type** | [**VolumeType**](VolumeType.md) |  | [optional] 
**storage_location** | **str** | The storage location of the volume | [optional] 
**full_name** | **str** | Full name of volume, in form of __catalog_name__.__schema_name__.__volume_name__. | [optional] 

## Example

```python
from unitycatalog.models.volume_info import VolumeInfo

# TODO update the JSON string below
json = "{}"
# create an instance of VolumeInfo from a JSON string
volume_info_instance = VolumeInfo.from_json(json)
# print the JSON string representation of the object
print(VolumeInfo.to_json())

# convert the object into a dict
volume_info_dict = volume_info_instance.to_dict()
# create an instance of VolumeInfo from a dict
volume_info_from_dict = VolumeInfo.from_dict(volume_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


