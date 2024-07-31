# UpdateSchema


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 
**new_name** | **str** | New name for the schema. | [optional] 

## Example

```python
from unitycatalog.models.update_schema import UpdateSchema

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateSchema from a JSON string
update_schema_instance = UpdateSchema.from_json(json)
# print the JSON string representation of the object
print(UpdateSchema.to_json())

# convert the object into a dict
update_schema_dict = update_schema_instance.to_dict()
# create an instance of UpdateSchema from a dict
update_schema_from_dict = UpdateSchema.from_dict(update_schema_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


