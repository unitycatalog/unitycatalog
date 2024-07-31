# TableInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of table, relative to parent schema. | [optional] 
**catalog_name** | **str** | Name of parent catalog. | [optional] 
**schema_name** | **str** | Name of parent schema relative to its parent catalog. | [optional] 
**table_type** | [**TableType**](TableType.md) |  | [optional] 
**data_source_format** | [**DataSourceFormat**](DataSourceFormat.md) |  | [optional] 
**columns** | [**List[ColumnInfo]**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the table&#39;s columns. | [optional] 
**storage_location** | **str** | Storage root URL for table (for **MANAGED**, **EXTERNAL** tables) | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 
**created_at** | **int** | Time at which this table was created, in epoch milliseconds. | [optional] 
**updated_at** | **int** | Time at which this table was last modified, in epoch milliseconds. | [optional] 
**table_id** | **str** | Unique identifier for the table. | [optional] 

## Example

```python
from unitycatalog.models.table_info import TableInfo

# TODO update the JSON string below
json = "{}"
# create an instance of TableInfo from a JSON string
table_info_instance = TableInfo.from_json(json)
# print the JSON string representation of the object
print(TableInfo.to_json())

# convert the object into a dict
table_info_dict = table_info_instance.to_dict()
# create an instance of TableInfo from a dict
table_info_from_dict = TableInfo.from_dict(table_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


