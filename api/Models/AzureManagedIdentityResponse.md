# AzureManagedIdentityResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **access\_connector\_id** | **String** | The Azure resource ID of the Azure Databricks Access Connector. Use the format /subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.Databricks/accessConnectors/{connector-name}. | [default to null] |
| **managed\_identity\_id** | **String** | The Azure resource ID of the managed identity. Use the format /subscriptions/{guid}/resourceGroups/{rg-name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity-name}. This is only available for user-assigned identities. For system-assigned identities, the access_connector_id is used to identify the identity. If this field is not provided, then we assume the AzureManagedIdentity is for a system-assigned identity.  | [optional] [default to null] |
| **credential\_id** | **String** | The Databricks internal ID that represents this managed identity. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

