# FunctionParameterInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of parameter. | 
**type_text** | **str** | Full data type spec, SQL/catalogString text. | 
**type_json** | **str** | Full data type spec, JSON-serialized. | 
**type_name** | [**ColumnTypeName**](ColumnTypeName.md) |  | 
**type_precision** | **int** | Digits of precision; required on Create for DecimalTypes. | [optional] 
**type_scale** | **int** | Digits to right of decimal; Required on Create for DecimalTypes. | [optional] 
**type_interval_type** | **str** | Format of IntervalType. | [optional] 
**position** | **int** | Ordinal position of column (starting at position 0). | 
**parameter_mode** | [**FunctionParameterMode**](FunctionParameterMode.md) |  | [optional] 
**parameter_type** | [**FunctionParameterType**](FunctionParameterType.md) |  | [optional] 
**parameter_default** | **str** | Default value of the parameter. | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 

## Example

```python
from unitycatalog.models.function_parameter_info import FunctionParameterInfo

# TODO update the JSON string below
json = "{}"
# create an instance of FunctionParameterInfo from a JSON string
function_parameter_info_instance = FunctionParameterInfo.from_json(json)
# print the JSON string representation of the object
print(FunctionParameterInfo.to_json())

# convert the object into a dict
function_parameter_info_dict = function_parameter_info_instance.to_dict()
# create an instance of FunctionParameterInfo from a dict
function_parameter_info_from_dict = FunctionParameterInfo.from_dict(function_parameter_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


