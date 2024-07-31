# CreateSchema


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of schema, relative to parent catalog. | 
**catalog_name** | **str** | Name of parent catalog. | 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 

## Example

```python
from unitycatalog.models.create_schema import CreateSchema

# TODO update the JSON string below
json = "{}"
# create an instance of CreateSchema from a JSON string
create_schema_instance = CreateSchema.from_json(json)
# print the JSON string representation of the object
print(CreateSchema.to_json())

# convert the object into a dict
create_schema_dict = create_schema_instance.to_dict()
# create an instance of CreateSchema from a dict
create_schema_from_dict = CreateSchema.from_dict(create_schema_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


