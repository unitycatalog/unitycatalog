# RegisteredModelInfo
## Properties

| Name                      | Type | Description                                                                                              | Notes |
|---------------------------| ------------- |----------------------------------------------------------------------------------------------------------| -------------|
| **name**                  | **String** | Name of the registered model, relative to the parent schema.                                             | [optional] [default to null] |
| **catalog\_name**         | **String** | Name of the parent catalog.                                                                              | [optional] [default to null] |
| **schema\_name**          | **String** | Name of the parent schema relative to its parent catalog.                                                | [optional] [default to null] |
| **storage\_location**     | **String** | URI of the underlying storage for the registered model.                                                  | [optional] [default to null] |
| **comment**               | **String** | User-provided free-form text description.                                                                | [optional] [default to null] |
| **full\_name**            | **String** | Full name of the registered model, in form of __catalog_name__.__schema_name__.__registered_model_name__ | [optional] [default to null] |
| **created\_at**           | **Long** | Time at which this schema was created, in epoch milliseconds.                                            | [optional] [default to null] |
| **created\_by**           | **String** | User that created the registered model.                                                                  | [optional] [default to null] |
| **updated\_at**           | **Long** | Time at which this schema was last modified, in epoch milliseconds.                                      | [optional] [default to null] |
| **updated\_by**           | **String** | User that last updated the registered model.                                                             | [optional] [default to null] |
| **registered\_model\_id** | **String** | Unique identifier for the schema.                                                                        | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

