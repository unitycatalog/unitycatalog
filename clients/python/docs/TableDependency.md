# TableDependency

A table that is dependent on a SQL object.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_full_name** | **str** | Full name of the dependent table, in the form of __catalog_name__.__schema_name__.__table_name__. | 

## Example

```python
from unitycatalog.models.table_dependency import TableDependency

# TODO update the JSON string below
json = "{}"
# create an instance of TableDependency from a JSON string
table_dependency_instance = TableDependency.from_json(json)
# print the JSON string representation of the object
print(TableDependency.to_json())

# convert the object into a dict
table_dependency_dict = table_dependency_instance.to_dict()
# create an instance of TableDependency from a dict
table_dependency_from_dict = TableDependency.from_dict(table_dependency_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


