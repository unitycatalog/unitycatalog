package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.unitycatalog.server.audit.Auditor;
import io.unitycatalog.server.audit.CapturingAuditSink;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.model.UpdateTable;
import io.unitycatalog.server.observability.UcMetric;
import io.unitycatalog.server.observability.UcMetricConstants;
import io.unitycatalog.server.observability.UnityCatalogMetrics;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.FileOperationsImpl;
import io.unitycatalog.server.service.credential.CachingCloudCredentialVendor;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialUse;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RepositoriesTest {

  private SessionFactory sessionFactory;
  @TempDatabase private TestDatabase testDatabase;
  private ServerProperties serverProperties;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    serverProperties = new ServerProperties(properties);
    sessionFactory = testDatabase.sessionFactory();
  }

  @Test
  void decoratorWrapsTheFileOperationsRepositoriesUse() {
    AtomicReference<FileOperations> decorated = new AtomicReference<>();
    UnaryOperator<FileOperations> decorator =
        delegate -> {
          FileOperations wrapper = new DelegatingFileOperations(delegate);
          decorated.set(wrapper);
          return wrapper;
        };

    Repositories repositories =
        new Repositories(
            sessionFactory, serverProperties, /* cloudCredentialVendor= */ null, decorator);

    // The decorator ran on the server-built default and its result is the instance repositories
    // expose (and hand to DeltaCommitRepository), so the seam is actually wired end to end.
    assertThat(decorated.get()).isInstanceOf(DelegatingFileOperations.class);
    assertThat(repositories.getFileOperations()).isSameAs(decorated.get());
    assertThat(((DelegatingFileOperations) decorated.get()).delegate)
        .isInstanceOf(FileOperationsImpl.class);
  }

  @Test
  void identityDecoratorLeavesTheDefaultFileOperationsInPlace() {
    Repositories repositories =
        new Repositories(
            sessionFactory,
            serverProperties,
            /* cloudCredentialVendor= */ null,
            UnaryOperator.identity());

    assertThat(repositories.getFileOperations()).isInstanceOf(FileOperationsImpl.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void defaultCloudCredentialVendorCachesOnlyWhenTheCacheIsEnabled(boolean cacheEnabled) {
    Properties properties = staticS3Properties(cacheEnabled);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    CloudCredentialVendor vendor =
        Repositories.defaultCloudCredentialVendor(
            new ServerProperties(properties), new UnityCatalogMetrics(registry));
    vendor.vendCredential(
        CredentialContext.create(
            NormalizedURL.from("s3://bucket/table"),
            Set.of(CredentialContext.Privilege.SELECT),
            Optional.empty()));

    assertThat(vendor.getClass())
        .isEqualTo(cacheEnabled ? CachingCloudCredentialVendor.class : CloudCredentialVendor.class);
    // The cache records into the metrics it was given.
    assertThat(cacheRequests(registry, UcMetricConstants.MISS)).isEqualTo(cacheEnabled ? 1.0 : 0.0);
  }

  /** A static per-bucket S3 credential, so a vend needs no cloud call. */
  private static Properties staticS3Properties(boolean cacheEnabled) {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    properties.setProperty("server.storage-credential-cache.enabled", String.valueOf(cacheEnabled));
    properties.setProperty("s3.bucketPath.0", "s3://bucket");
    properties.setProperty("s3.accessKey.0", "AK");
    properties.setProperty("s3.secretKey.0", "SK");
    properties.setProperty("s3.sessionToken.0", "TK");
    return properties;
  }

  private static double cacheRequests(SimpleMeterRegistry registry, String result) {
    return TestUtils.counterCount(
        registry, UcMetric.STORAGE_CREDENTIAL_CACHE_REQUESTS, UcMetricConstants.RESULT, result);
  }

  @Test
  void auditRecordsCredentialsServedFromTheCache() {
    Properties properties = staticS3Properties(/* cacheEnabled= */ true);
    properties.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    ServerProperties audited = new ServerProperties(properties);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CapturingAuditSink.reset();
    Repositories repositories =
        new Repositories(
            sessionFactory,
            audited,
            /* cloudCredentialVendor= */ null,
            UnaryOperator.identity(),
            new UnityCatalogMetrics(registry),
            Auditor.fromProperties(audited));
    NormalizedURL path = NormalizedURL.from("s3://bucket/table");

    for (int i = 0; i < 2; i++) {
      repositories
          .getStorageCredentialVendor()
          .vendCredential(path, CredentialContext.READ_ONLY, CredentialUse.CLIENT);
    }

    // The second vend is a cache hit and is recorded too.
    assertThat(cacheRequests(registry, UcMetricConstants.MISS)).isEqualTo(1.0);
    assertThat(cacheRequests(registry, UcMetricConstants.HIT)).isEqualTo(1.0);
    assertThat(CapturingAuditSink.RECORDS).hasSize(2);
  }

  @Test
  void injectedCloudCredentialVendorIsUsedAsIs() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    properties.setProperty("server.storage-credential-cache.enabled", "true");
    CloudCredentialVendor injected = mock(CloudCredentialVendor.class);
    when(injected.vendCredential(any()))
        .thenReturn(
            new TemporaryCredentials()
                .expirationTime(System.currentTimeMillis() + Duration.ofHours(1).toMillis()));
    Repositories repositories =
        new Repositories(sessionFactory, new ServerProperties(properties), injected);
    NormalizedURL path = NormalizedURL.from("s3://bucket/table");

    repositories
        .getStorageCredentialVendor()
        .vendCredential(path, CredentialContext.READ_ONLY, CredentialUse.CLIENT);
    repositories
        .getStorageCredentialVendor()
        .vendCredential(path, CredentialContext.READ_ONLY, CredentialUse.CLIENT);

    // Even with the cache enabled, an injected vendor is not wrapped: every request reaches it.
    verify(injected, times(2)).vendCredential(any());
  }

  /** Every catalog, schema and table read that returns the API model carries its properties. */
  @Test
  void catalogSchemaAndTableReadsAttachTheirProperties() {
    UUID catalogId = UUID.randomUUID();
    UUID schemaId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    Map<String, String> catalogProperties = Map.of("level", "catalog", "tier", "gold");
    Map<String, String> schemaProperties = Map.of("level", "schema");
    Map<String, String> tableProperties = Map.of("level", "table", "team", "ml");
    sessionFactory.inTransaction(
        session -> {
          session.persist(
              UserDAO.builder()
                  .id(UUID.randomUUID())
                  .name("owner@localhost")
                  .email("owner@localhost")
                  .state("ENABLED")
                  .createdAt(new Date())
                  .build());
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name("cat").createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaId)
                  .name("sch")
                  .catalogId(catalogId)
                  .createdAt(new Date())
                  .build());
          session.persist(
              TableInfoDAO.builder()
                  .id(tableId)
                  .name("tbl")
                  .schemaId(schemaId)
                  .type("EXTERNAL")
                  .dataSourceFormat("DELTA")
                  .url("s3://bucket/tbl")
                  .createdAt(new Date())
                  .build());
          PropertyDAO.from(catalogProperties, catalogId, Constants.CATALOG)
              .forEach(session::persist);
          PropertyDAO.from(schemaProperties, schemaId, Constants.SCHEMA).forEach(session::persist);
          PropertyDAO.from(tableProperties, tableId, Constants.TABLE).forEach(session::persist);
        });
    Repositories repositories = new Repositories(sessionFactory, serverProperties);
    CatalogRepository catalogs = repositories.getCatalogRepository();
    SchemaRepository schemas = repositories.getSchemaRepository();
    TableRepository tables = repositories.getTableRepository();

    try (SafeCloseable ignored =
        ServiceRequestContext.of(HttpRequest.of(HttpMethod.PATCH, "/")).push()) {
      assertThat(
              List.of(
                  catalogs.getCatalog("cat"),
                  catalogs.listCatalogs(Optional.empty(), Optional.empty()).getCatalogs().get(0),
                  catalogs.updateCatalog("cat", new UpdateCatalog()),
                  catalogs.updateCatalog("cat", new UpdateCatalog().comment("changed"))))
          .extracting(CatalogInfo::getProperties)
          .containsOnly(catalogProperties);
      assertThat(
              List.of(
                  schemas.getSchema("cat.sch"),
                  schemas
                      .listSchemas("cat", Optional.empty(), Optional.empty())
                      .getSchemas()
                      .get(0),
                  schemas.updateSchema("cat.sch", new UpdateSchema()),
                  schemas.updateSchema("cat.sch", new UpdateSchema().comment("changed"))))
          .extracting(SchemaInfo::getProperties)
          .containsOnly(schemaProperties);
      assertThat(
              List.of(
                  tables.getTable("cat.sch.tbl"),
                  tables.updateTable(
                      "cat.sch.tbl",
                      new UpdateTable().owner("owner@localhost"),
                      /* canTransferViewToRecipient= */ false,
                      () -> {})))
          .extracting(TableInfo::getProperties)
          .containsOnly(tableProperties);
    }
  }

  /** Test wrapper that records its delegate; methods forward, but the tests only check identity. */
  private static final class DelegatingFileOperations implements FileOperations {
    private final FileOperations delegate;

    DelegatingFileOperations(FileOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public FileIO getFileIO(NormalizedURL rootPath, Set<CredentialContext.Privilege> privileges) {
      return delegate.getFileIO(rootPath, privileges);
    }

    @Override
    public SupportsPrefixOperations getCleanupFileIO(
        NormalizedURL rootPath, UUID resourceId, CooperativeDeadline deadline) {
      return delegate.getCleanupFileIO(rootPath, resourceId, deadline);
    }

    @Override
    public Map<String, String> getFileIOConfig(
        NormalizedURL path,
        Set<CredentialContext.Privilege> privileges,
        Optional<String> credentialsEndpoint) {
      return delegate.getFileIOConfig(path, privileges, credentialsEndpoint);
    }
  }
}
