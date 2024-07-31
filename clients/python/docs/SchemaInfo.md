# SchemaInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of schema, relative to parent catalog. | [optional] 
**catalog_name** | **str** | Name of parent catalog. | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 
**full_name** | **str** | Full name of schema, in form of __catalog_name__.__schema_name__. | [optional] 
**created_at** | **int** | Time at which this schema was created, in epoch milliseconds. | [optional] 
**updated_at** | **int** | Time at which this schema was last modified, in epoch milliseconds. | [optional] 
**schema_id** | **str** | Unique identifier for the schema. | [optional] 

## Example

```python
from unitycatalog.models.schema_info import SchemaInfo

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaInfo from a JSON string
schema_info_instance = SchemaInfo.from_json(json)
# print the JSON string representation of the object
print(SchemaInfo.to_json())

# convert the object into a dict
schema_info_dict = schema_info_instance.to_dict()
# create an instance of SchemaInfo from a dict
schema_info_from_dict = SchemaInfo.from_dict(schema_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


