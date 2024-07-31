# FunctionParameterInfos


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**parameters** | [**List[FunctionParameterInfo]**](FunctionParameterInfo.md) | The array of __FunctionParameterInfo__ definitions of the function&#39;s parameters. | [optional] 

## Example

```python
from unitycatalog.models.function_parameter_infos import FunctionParameterInfos

# TODO update the JSON string below
json = "{}"
# create an instance of FunctionParameterInfos from a JSON string
function_parameter_infos_instance = FunctionParameterInfos.from_json(json)
# print the JSON string representation of the object
print(FunctionParameterInfos.to_json())

# convert the object into a dict
function_parameter_infos_dict = function_parameter_infos_instance.to_dict()
# create an instance of FunctionParameterInfos from a dict
function_parameter_infos_from_dict = FunctionParameterInfos.from_dict(function_parameter_infos_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


