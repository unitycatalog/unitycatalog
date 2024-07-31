# CreateTable


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of table, relative to parent schema. | 
**catalog_name** | **str** | Name of parent catalog. | 
**schema_name** | **str** | Name of parent schema relative to its parent catalog. | 
**table_type** | [**TableType**](TableType.md) |  | 
**data_source_format** | [**DataSourceFormat**](DataSourceFormat.md) |  | 
**columns** | [**List[ColumnInfo]**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the table&#39;s columns. | 
**storage_location** | **str** | Storage root URL for table (for **MANAGED**, **EXTERNAL** tables) | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 

## Example

```python
from unitycatalog.models.create_table import CreateTable

# TODO update the JSON string below
json = "{}"
# create an instance of CreateTable from a JSON string
create_table_instance = CreateTable.from_json(json)
# print the JSON string representation of the object
print(CreateTable.to_json())

# convert the object into a dict
create_table_dict = create_table_instance.to_dict()
# create an instance of CreateTable from a dict
create_table_from_dict = CreateTable.from_dict(create_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


