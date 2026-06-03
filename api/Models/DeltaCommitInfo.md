# DeltaCommitInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **version** | **Long** | required | The version of this commit. | |
| **timestamp** | **Long** | required | The timestamp for when the commit was made. This is the in-commit timestamp as produced by the Delta client writing to the table. | |
| **file\_name** | **String** | required | The filename of the UUID-based commit file. | |
| **file\_size** | **Long** | required | The size of the commit file in bytes. | |
| **file\_modification\_timestamp** | **Long** | required | The modification time of the commit file. This is the mod time of the file as written to the file system. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

