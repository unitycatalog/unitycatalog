# DependencyList

A list of dependencies.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**dependencies** | [**List[Dependency]**](Dependency.md) | Array of dependencies. | [optional] 

## Example

```python
from unitycatalog.models.dependency_list import DependencyList

# TODO update the JSON string below
json = "{}"
# create an instance of DependencyList from a JSON string
dependency_list_instance = DependencyList.from_json(json)
# print the JSON string representation of the object
print(DependencyList.to_json())

# convert the object into a dict
dependency_list_dict = dependency_list_instance.to_dict()
# create an instance of DependencyList from a dict
dependency_list_from_dict = DependencyList.from_dict(dependency_list_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


