# FunctionDependency

A function that is dependent on a SQL object.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**function_full_name** | **str** | Full name of the dependent function, in the form of __catalog_name__.__schema_name__.__function_name__. | 

## Example

```python
from unitycatalog.models.function_dependency import FunctionDependency

# TODO update the JSON string below
json = "{}"
# create an instance of FunctionDependency from a JSON string
function_dependency_instance = FunctionDependency.from_json(json)
# print the JSON string representation of the object
print(FunctionDependency.to_json())

# convert the object into a dict
function_dependency_dict = function_dependency_instance.to_dict()
# create an instance of FunctionDependency from a dict
function_dependency_from_dict = FunctionDependency.from_dict(function_dependency_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


