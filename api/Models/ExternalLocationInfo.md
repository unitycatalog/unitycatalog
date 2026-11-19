# ExternalLocationInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **browse\_only** | **Boolean** | True when the response contains only discovery metadata authorized by BROWSE. | [optional] [default to null] |
| **name** | **String** | Name of the external location. | [optional] [default to null] |
| **id** | **String** | Unique identifier for the external location. | [optional] [default to null] |
| **url** | **String** | Path URL of the external location. | [optional] [default to null] |
| **credential\_name** | **String** | Name of the storage credential used with this location. Omitted when the credential no longer exists, for example after it was force-deleted. Updating the external location with a new credential_name repairs it. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **owner** | **String** | The owner of the external location. | [optional] [default to null] |
| **credential\_id** | **String** | Unique ID of the location&#39;s storage credential. | [optional] [default to null] |
| **endpoint** | **String** | EXPERIMENTAL. S3 API endpoint of the S3-compatible storage the external location is on, for example &#x60;https://minio.example.com:9000&#x60;: an http or https URL with a host, and without user info, a query or a fragment; other values fail with &#x60;INVALID_ARGUMENT&#x60;. Set exactly when the location&#39;s credential is &#x60;s3_compatible_access_key&#x60;. Credential vending returns it with the region and the addressing style engines need to reach the storage.  | [optional] [default to null] |
| **addressing\_style** | [**S3AddressingStyle**](S3AddressingStyle.md) |  | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this external location was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of external location creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which external location this was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified the external location. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

