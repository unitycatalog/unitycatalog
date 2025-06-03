- server/src/main/java/io/unitycatalog/server/utils/Constants.java
- server/src/main/java/io/unitycatalog/server/persist/utils/FileOperations.java
    - deleteDirectory function -> needs to handle other schemes also
    - modifyS3Directory function -> to handle R2 correctly
- server/src/main/java/io/unitycatalog/server/persist/utils/UriUtils.java
    - add support for R2
- server/src/main/java/io/unitycatalog/server/service/credential/CloudCredentialVendor.java
    - add support for R2
- server/src/main/java/io/unitycatalog/server/service/iceberg/FileIOFactory.java
    - can skip; related to iceberg
- server/src/main/java/io/unitycatalog/server/service/iceberg/TableConfigService.java
    - can skip; related to iceberg
- connectors/spark/src/main/scala/io/unitycatalog/spark/UCSingleCatalog.scala
    - add the new R2 credential vendor and the new scheme
- connectors/spark/src/test/java/io/unitycatalog/spark/BaseSparkIntegrationTest.java
    - can skip; related to tests
- etc/conf/server.properties
    - enable configs for R2
- examples/cli/src/main/java/io/unitycatalog/cli/TableCli.java
    - update for R2 especially in the handleTableStorageLocation function
- examples/cli/src/main/java/io/unitycatalog/cli/delta/DeltaKernelUtils.java
    - skip for now; not sure what is it related to
- server/src/test/java/io/unitycatalog/server/base/BaseCRUDTestWithMockCredentials.java
    - skip; related to tests
- server/src/test/java/io/unitycatalog/server/sdk/tempcredential/SdkTemporaryModelVersionCredentialTest.java
    - skip; related to tests
- server/src/test/java/io/unitycatalog/server/service/credential/CloudCredentialVendorTest.java
    - skip; related to tests
- server/src/test/java/io/unitycatalog/server/service/credential/aws/AwsPolicyGeneratorTest.java
    - skip; related to tests
- server/src/test/java/io/unitycatalog/server/service/iceberg/MetadataServiceTest.java
    - skip; related to tests
- server/src/test/resources/simple-v1-iceberg.metadata.json
    - skip; related to iceberg, tests
- server/src/main/java/io/unitycatalog/server/UnityCatalogServer.java
    - update this file to capture R2 creds from server.properties
- server/src/main/java/io/unitycatalog/server/utils/ServerProperties.java
    - update this file to capture R2 creds from server.properties
    - introduce s3.endpoint property also
- how to make sure that even empty storage location works???




# Changes done
- build.sbt: included org.json
- server/src/main/java/io/unitycatalog/server/service/credential/aws/AwsCredentialVendor.java
- S3StorageConfig.java -- added enpoint to schema
- server/src/main/java/io/unitycatalog/server/utils/ServerProperties.java -- handled endpoint for s3 in the config
- 



