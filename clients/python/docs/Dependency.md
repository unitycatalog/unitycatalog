# Dependency

A dependency of a SQL object. Either the __table__ field or the __function__ field must be defined.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table** | [**TableDependency**](TableDependency.md) |  | [optional] 
**function** | [**FunctionDependency**](FunctionDependency.md) |  | [optional] 

## Example

```python
from unitycatalog.models.dependency import Dependency

# TODO update the JSON string below
json = "{}"
# create an instance of Dependency from a JSON string
dependency_instance = Dependency.from_json(json)
# print the JSON string representation of the object
print(Dependency.to_json())

# convert the object into a dict
dependency_dict = dependency_instance.to_dict()
# create an instance of Dependency from a dict
dependency_from_dict = Dependency.from_dict(dependency_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


