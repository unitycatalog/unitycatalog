# ListVolumesResponseContent


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**volumes** | [**List[VolumeInfo]**](VolumeInfo.md) |  | [optional] 
**next_page_token** | **str** | Opaque token to retrieve the next page of results. Absent if there are no more pages. __page_token__ should be set to this value for the next request to retrieve the next page of results.  | [optional] 

## Example

```python
from unitycatalog.models.list_volumes_response_content import ListVolumesResponseContent

# TODO update the JSON string below
json = "{}"
# create an instance of ListVolumesResponseContent from a JSON string
list_volumes_response_content_instance = ListVolumesResponseContent.from_json(json)
# print the JSON string representation of the object
print(ListVolumesResponseContent.to_json())

# convert the object into a dict
list_volumes_response_content_dict = list_volumes_response_content_instance.to_dict()
# create an instance of ListVolumesResponseContent from a dict
list_volumes_response_content_from_dict = ListVolumesResponseContent.from_dict(list_volumes_response_content_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


