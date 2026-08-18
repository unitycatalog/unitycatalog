# Documentation for UC Delta API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *DeltaConfigurationApi* | [**getConfig**](Apis/DeltaConfigurationApi.md#getconfig) | **GET** /delta/v1/config | Get catalog configuration |
| *DeltaTablesApi* | [**createStagingTable**](Apis/DeltaTablesApi.md#createstagingtable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables | Create a staging table |
*DeltaTablesApi* | [**createTable**](Apis/DeltaTablesApi.md#createtable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables | Create a table |
*DeltaTablesApi* | [**deleteTable**](Apis/DeltaTablesApi.md#deletetable) | **DELETE** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Delete a table |
*DeltaTablesApi* | [**loadTable**](Apis/DeltaTablesApi.md#loadtable) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Load table metadata |
*DeltaTablesApi* | [**renameTable**](Apis/DeltaTablesApi.md#renametable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename | Rename a table |
*DeltaTablesApi* | [**reportMetrics**](Apis/DeltaTablesApi.md#reportmetrics) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics | Report commit metrics |
*DeltaTablesApi* | [**tableExists**](Apis/DeltaTablesApi.md#tableexists) | **HEAD** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Check if table exists |
*DeltaTablesApi* | [**updateTable**](Apis/DeltaTablesApi.md#updatetable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Update table |
| *DeltaTemporaryCredentialsApi* | [**getStagingTableCredentials**](Apis/DeltaTemporaryCredentialsApi.md#getstagingtablecredentials) | **GET** /delta/v1/staging-tables/{table_id}/credentials | Get staging table credentials by UUID |
*DeltaTemporaryCredentialsApi* | [**getTableCredentials**](Apis/DeltaTemporaryCredentialsApi.md#gettablecredentials) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials | Get table credentials |
*DeltaTemporaryCredentialsApi* | [**getTemporaryPathCredentials**](Apis/DeltaTemporaryCredentialsApi.md#gettemporarypathcredentials) | **GET** /delta/v1/temporary-path-credentials | Get temporary path credentials |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [DeltaAddCommitUpdate](./Models/DeltaAddCommitUpdate.md)
 - [DeltaArrayType](./Models/DeltaArrayType.md)
 - [DeltaAssertEtag](./Models/DeltaAssertEtag.md)
 - [DeltaAssertTableUUID](./Models/DeltaAssertTableUUID.md)
 - [DeltaCatalogConfig](./Models/DeltaCatalogConfig.md)
 - [DeltaClusteringDomainMetadata](./Models/DeltaClusteringDomainMetadata.md)
 - [DeltaCommit](./Models/DeltaCommit.md)
 - [DeltaCreateStagingTableRequest](./Models/DeltaCreateStagingTableRequest.md)
 - [DeltaCreateTableRequest](./Models/DeltaCreateTableRequest.md)
 - [DeltaCredentialOperation](./Models/DeltaCredentialOperation.md)
 - [DeltaCredentialsResponse](./Models/DeltaCredentialsResponse.md)
 - [DeltaDataType](./Models/DeltaDataType.md)
 - [DeltaDecimalType](./Models/DeltaDecimalType.md)
 - [DeltaDomainMetadataUpdates](./Models/DeltaDomainMetadataUpdates.md)
 - [DeltaErrorModel](./Models/DeltaErrorModel.md)
 - [DeltaErrorResponse](./Models/DeltaErrorResponse.md)
 - [DeltaErrorType](./Models/DeltaErrorType.md)
 - [DeltaLoadTableResponse](./Models/DeltaLoadTableResponse.md)
 - [DeltaMapType](./Models/DeltaMapType.md)
 - [DeltaPrimitiveType](./Models/DeltaPrimitiveType.md)
 - [DeltaProtocol](./Models/DeltaProtocol.md)
 - [DeltaRemoveDomainMetadataUpdate](./Models/DeltaRemoveDomainMetadataUpdate.md)
 - [DeltaRemovePropertiesUpdate](./Models/DeltaRemovePropertiesUpdate.md)
 - [DeltaRenameTableRequest](./Models/DeltaRenameTableRequest.md)
 - [DeltaReportMetricsRequest](./Models/DeltaReportMetricsRequest.md)
 - [DeltaReportMetricsRequest_report](./Models/DeltaReportMetricsRequest_report.md)
 - [DeltaReportMetricsRequest_report_commit_report](./Models/DeltaReportMetricsRequest_report_commit_report.md)
 - [DeltaReportMetricsRequest_report_commit_report_file_size_histogram](./Models/DeltaReportMetricsRequest_report_commit_report_file_size_histogram.md)
 - [DeltaRowTrackingDomainMetadata](./Models/DeltaRowTrackingDomainMetadata.md)
 - [DeltaSetDomainMetadataUpdate](./Models/DeltaSetDomainMetadataUpdate.md)
 - [DeltaSetLatestBackfilledVersionUpdate](./Models/DeltaSetLatestBackfilledVersionUpdate.md)
 - [DeltaSetPartitionColumnsUpdate](./Models/DeltaSetPartitionColumnsUpdate.md)
 - [DeltaSetPropertiesUpdate](./Models/DeltaSetPropertiesUpdate.md)
 - [DeltaSetProtocolUpdate](./Models/DeltaSetProtocolUpdate.md)
 - [DeltaSetSchemaUpdate](./Models/DeltaSetSchemaUpdate.md)
 - [DeltaSetTableCommentUpdate](./Models/DeltaSetTableCommentUpdate.md)
 - [DeltaStagingTableResponse](./Models/DeltaStagingTableResponse.md)
 - [DeltaStagingTableResponse_required_protocol](./Models/DeltaStagingTableResponse_required_protocol.md)
 - [DeltaStagingTableResponse_suggested_protocol](./Models/DeltaStagingTableResponse_suggested_protocol.md)
 - [DeltaStorageCredential](./Models/DeltaStorageCredential.md)
 - [DeltaStorageCredential_config](./Models/DeltaStorageCredential_config.md)
 - [DeltaStructField](./Models/DeltaStructField.md)
 - [DeltaStructFieldMetadata](./Models/DeltaStructFieldMetadata.md)
 - [DeltaStructType](./Models/DeltaStructType.md)
 - [DeltaTableMetadata](./Models/DeltaTableMetadata.md)
 - [DeltaTableRequirement](./Models/DeltaTableRequirement.md)
 - [DeltaTableType](./Models/DeltaTableType.md)
 - [DeltaTableUpdate](./Models/DeltaTableUpdate.md)
 - [DeltaUniformMetadata](./Models/DeltaUniformMetadata.md)
 - [DeltaUniformMetadata_iceberg](./Models/DeltaUniformMetadata_iceberg.md)
 - [DeltaUpdateSnapshotVersionUpdate](./Models/DeltaUpdateSnapshotVersionUpdate.md)
 - [DeltaUpdateTableRequest](./Models/DeltaUpdateTableRequest.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
