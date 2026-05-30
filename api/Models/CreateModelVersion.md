# CreateModelVersion
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **model\_name** | **String** | required | Name of registered model, relative to parent schema. | |
| **catalog\_name** | **String** | required | Name of parent catalog. | |
| **schema\_name** | **String** | required | Name of parent schema relative to its parent catalog. | |
| **source** | **String** | required | URI indicating the location of the source model artifacts. | |
| **run\_id** | **String** | optional | The run id used by the ML package that generated the model. | |
| **comment** | **String** | optional | User-provided free-form text description. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

