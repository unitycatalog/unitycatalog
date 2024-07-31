# UpdateCatalog


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 
**new_name** | **str** | New name for the catalog. | [optional] 

## Example

```python
from unitycatalog.models.update_catalog import UpdateCatalog

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateCatalog from a JSON string
update_catalog_instance = UpdateCatalog.from_json(json)
# print the JSON string representation of the object
print(UpdateCatalog.to_json())

# convert the object into a dict
update_catalog_dict = update_catalog_instance.to_dict()
# create an instance of UpdateCatalog from a dict
update_catalog_from_dict = UpdateCatalog.from_dict(update_catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


