# Documentation for Delta REST Catalog API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *ConfigurationApi* | [**getConfig**](Apis/ConfigurationApi.md#getconfig) | **GET** /delta/v1/config | Get catalog configuration |
| *TablesApi* | [**createStagingTable**](Apis/TablesApi.md#createstagingtable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables | Create a staging table |
*TablesApi* | [**createTable**](Apis/TablesApi.md#createtable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables | Create a table |
*TablesApi* | [**deleteTable**](Apis/TablesApi.md#deletetable) | **DELETE** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Delete a table |
*TablesApi* | [**listTables**](Apis/TablesApi.md#listtables) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables | List tables |
*TablesApi* | [**loadTable**](Apis/TablesApi.md#loadtable) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Load table metadata |
*TablesApi* | [**renameTable**](Apis/TablesApi.md#renametable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename | Rename a table |
*TablesApi* | [**reportMetrics**](Apis/TablesApi.md#reportmetrics) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics | Report commit metrics |
*TablesApi* | [**tableExists**](Apis/TablesApi.md#tableexists) | **HEAD** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Check if table exists |
*TablesApi* | [**updateTable**](Apis/TablesApi.md#updatetable) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table} | Update table |
| *TemporaryCredentialsApi* | [**getStagingTableCredentials**](Apis/TemporaryCredentialsApi.md#getstagingtablecredentials) | **GET** /delta/v1/staging-tables/{table_id}/credentials | Get staging table credentials by UUID |
*TemporaryCredentialsApi* | [**getTableCredentials**](Apis/TemporaryCredentialsApi.md#gettablecredentials) | **GET** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials | Get table credentials |
*TemporaryCredentialsApi* | [**getTemporaryPathCredentials**](Apis/TemporaryCredentialsApi.md#gettemporarypathcredentials) | **GET** /delta/v1/temporary-path-credentials | Get temporary path credentials |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AddCommitUpdate](./Models/AddCommitUpdate.md)
 - [ArrayType](./Models/ArrayType.md)
 - [AssertEtag](./Models/AssertEtag.md)
 - [AssertTableUUID](./Models/AssertTableUUID.md)
 - [CatalogConfig](./Models/CatalogConfig.md)
 - [ClusteringDomainMetadata](./Models/ClusteringDomainMetadata.md)
 - [CreateStagingTableRequest](./Models/CreateStagingTableRequest.md)
 - [CreateTableRequest](./Models/CreateTableRequest.md)
 - [CredentialOperation](./Models/CredentialOperation.md)
 - [CredentialsResponse](./Models/CredentialsResponse.md)
 - [DataSourceFormat](./Models/DataSourceFormat.md)
 - [DecimalType](./Models/DecimalType.md)
 - [DeltaCommit](./Models/DeltaCommit.md)
 - [DeltaProtocol](./Models/DeltaProtocol.md)
 - [DeltaType](./Models/DeltaType.md)
 - [DomainMetadataUpdates](./Models/DomainMetadataUpdates.md)
 - [ErrorModel](./Models/ErrorModel.md)
 - [ErrorResponse](./Models/ErrorResponse.md)
 - [ErrorType](./Models/ErrorType.md)
 - [ListTablesResponse](./Models/ListTablesResponse.md)
 - [LoadTableResponse](./Models/LoadTableResponse.md)
 - [MapType](./Models/MapType.md)
 - [PrimitiveType](./Models/PrimitiveType.md)
 - [RemoveDomainMetadataUpdate](./Models/RemoveDomainMetadataUpdate.md)
 - [RemovePropertiesUpdate](./Models/RemovePropertiesUpdate.md)
 - [RenameTableRequest](./Models/RenameTableRequest.md)
 - [ReportMetricsRequest](./Models/ReportMetricsRequest.md)
 - [ReportMetricsRequest_report](./Models/ReportMetricsRequest_report.md)
 - [ReportMetricsRequest_report_commit_report](./Models/ReportMetricsRequest_report_commit_report.md)
 - [ReportMetricsRequest_report_commit_report_file_size_histogram](./Models/ReportMetricsRequest_report_commit_report_file_size_histogram.md)
 - [RowTrackingDomainMetadata](./Models/RowTrackingDomainMetadata.md)
 - [SetDomainMetadataUpdate](./Models/SetDomainMetadataUpdate.md)
 - [SetLatestBackfilledVersionUpdate](./Models/SetLatestBackfilledVersionUpdate.md)
 - [SetPartitionColumnsUpdate](./Models/SetPartitionColumnsUpdate.md)
 - [SetPropertiesUpdate](./Models/SetPropertiesUpdate.md)
 - [SetProtocolUpdate](./Models/SetProtocolUpdate.md)
 - [SetSchemaUpdate](./Models/SetSchemaUpdate.md)
 - [SetTableCommentUpdate](./Models/SetTableCommentUpdate.md)
 - [StagingTableResponse](./Models/StagingTableResponse.md)
 - [StagingTableResponse_required_protocol](./Models/StagingTableResponse_required_protocol.md)
 - [StagingTableResponse_suggested_protocol](./Models/StagingTableResponse_suggested_protocol.md)
 - [StorageCredential](./Models/StorageCredential.md)
 - [StorageCredential_config](./Models/StorageCredential_config.md)
 - [StructField](./Models/StructField.md)
 - [StructType](./Models/StructType.md)
 - [TableIdentifierWithDataSourceFormat](./Models/TableIdentifierWithDataSourceFormat.md)
 - [TableMetadata](./Models/TableMetadata.md)
 - [TableRequirement](./Models/TableRequirement.md)
 - [TableType](./Models/TableType.md)
 - [TableUpdate](./Models/TableUpdate.md)
 - [UniformMetadata](./Models/UniformMetadata.md)
 - [UniformMetadata_iceberg](./Models/UniformMetadata_iceberg.md)
 - [UpdateSnapshotVersionUpdate](./Models/UpdateSnapshotVersionUpdate.md)
 - [UpdateTableRequest](./Models/UpdateTableRequest.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
