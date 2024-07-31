# ColumnInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of Column. | [optional] 
**type_text** | **str** | Full data type specification as SQL/catalogString text. | [optional] 
**type_json** | **str** | Full data type specification, JSON-serialized. | [optional] 
**type_name** | [**ColumnTypeName**](ColumnTypeName.md) |  | [optional] 
**type_precision** | **int** | Digits of precision; required for DecimalTypes. | [optional] 
**type_scale** | **int** | Digits to right of decimal; Required for DecimalTypes. | [optional] 
**type_interval_type** | **str** | Format of IntervalType. | [optional] 
**position** | **int** | Ordinal position of column (starting at position 0). | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**nullable** | **bool** | Whether field may be Null. | [optional] [default to True]
**partition_index** | **int** | Partition index for column. | [optional] 

## Example

```python
from unitycatalog.models.column_info import ColumnInfo

# TODO update the JSON string below
json = "{}"
# create an instance of ColumnInfo from a JSON string
column_info_instance = ColumnInfo.from_json(json)
# print the JSON string representation of the object
print(ColumnInfo.to_json())

# convert the object into a dict
column_info_dict = column_info_instance.to_dict()
# create an instance of ColumnInfo from a dict
column_info_from_dict = ColumnInfo.from_dict(column_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


