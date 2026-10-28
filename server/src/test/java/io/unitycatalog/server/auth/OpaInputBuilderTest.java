package io.unitycatalog.server.auth;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpaInputBuilderTest {
  private final UserDAO user =
      UserDAO.builder().id(UUID.randomUUID()).name("Test user").email("user@example.com").build();
  private final CatalogInfoDAO catalog =
      CatalogInfoDAO.builder()
          .id(UUID.randomUUID())
          .name("catalog")
          .owner("owner@example.com")
          .build();
  private final SchemaInfoDAO schema =
      SchemaInfoDAO.builder()
          .id(UUID.randomUUID())
          .name("schema")
          .catalogId(catalog.getId())
          .owner("owner@example.com")
          .build();

  private TestDatabase testDatabase;
  private HibernateConfigurator hibernateConfigurator;
  private ServerProperties serverProperties;
  private OpaInputBuilder inputBuilder;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    properties.setProperty(Property.POLICY_REFRESH_ENABLED.getKey(), "false");
    serverProperties = new ServerProperties(properties);
    testDatabase = TestDatabases.newDatabase();
    hibernateConfigurator = testDatabase.openHibernateConfigurator(serverProperties);
    inputBuilder = new OpaInputBuilder(hibernateConfigurator.getSessionFactory());
    persist(user, catalog, schema);
  }

  @AfterEach
  void tearDown() {
    testDatabase.close();
  }

  @Test
  void buildsAllResourceTypesWithoutNativeGrantsOwnersOrCredentials() throws Exception {
    MetastoreDAO metastore = MetastoreDAO.builder().id(UUID.randomUUID()).build();
    Map<SecurableType, IdentifiableDAO> dataResources =
        Map.of(
            TABLE,
            TableInfoDAO.builder()
                .id(UUID.randomUUID())
                .name("table")
                .schemaId(schema.getId())
                .owner("owner@example.com")
                .build(),
            VOLUME,
            VolumeInfoDAO.builder()
                .id(UUID.randomUUID())
                .name("volume")
                .schemaId(schema.getId())
                .owner("owner@example.com")
                .build(),
            FUNCTION,
            FunctionInfoDAO.builder()
                .id(UUID.randomUUID())
                .name("function")
                .schemaId(schema.getId())
                .owner("owner@example.com")
                .build(),
            REGISTERED_MODEL,
            RegisteredModelInfoDAO.builder()
                .id(UUID.randomUUID())
                .name("model")
                .schemaId(schema.getId())
                .owner("owner@example.com")
                .build());
    StagingTableDAO staging =
        StagingTableDAO.builder()
            .id(UUID.randomUUID())
            .name("staged_table")
            .schemaId(schema.getId())
            .stagingLocation("s3://test-bucket/staging")
            .build();
    staging.setDefaultFields();
    CredentialDAO credential =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("credential")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/test")),
            "owner@example.com");
    ExternalLocationDAO location =
        ExternalLocationDAO.builder()
            .id(UUID.randomUUID())
            .name("location")
            .credentialId(credential.getId())
            .url("s3://test-bucket/location")
            .owner("owner@example.com")
            .build();
    persist(metastore, staging, credential, location);
    persist(dataResources.values().toArray());
    try (JCasbinAuthorizer authorizer =
        new JCasbinAuthorizer(hibernateConfigurator, serverProperties)) {
      authorizer.grantAuthorization(user.getId(), schema.getId(), Privileges.CREATE_TABLE);
      authorizer.grantAuthorization(user.getId(), credential.getId(), Privileges.OWNER);
    }

    assertInput(
        METASTORE,
        metastore.getId(),
        Map.of("id", metastore.getId().toString(), "type", "METASTORE", "parents", List.of()));
    assertInput(CATALOG, catalog, List.of());
    assertInput(SCHEMA, schema, List.of(identity(CATALOG, catalog)));
    List<Map<String, String>> parents =
        List.of(identity(SCHEMA, schema), identity(CATALOG, catalog));
    dataResources.forEach((type, resource) -> assertInput(type, resource, parents));
    assertInput(TABLE, staging, parents);
    assertInput(CREDENTIAL, credential, List.of());
    assertInput(EXTERNAL_LOCATION, location, List.of());
  }

  @Test
  void distinguishesNamesByParentsAndIncludesOptionalExternalId() {
    CatalogInfoDAO otherCatalog =
        CatalogInfoDAO.builder().id(UUID.randomUUID()).name("other_catalog").build();
    SchemaInfoDAO otherSchema =
        SchemaInfoDAO.builder()
            .id(UUID.randomUUID())
            .name(schema.getName())
            .catalogId(otherCatalog.getId())
            .build();
    TableInfoDAO table =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("orders")
            .schemaId(schema.getId())
            .build();
    TableInfoDAO otherTable =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name(table.getName())
            .schemaId(otherSchema.getId())
            .build();
    persist(otherCatalog, otherSchema, table, otherTable);

    assertInput(SCHEMA, schema, List.of(identity(CATALOG, catalog)));
    assertInput(SCHEMA, otherSchema, List.of(identity(CATALOG, otherCatalog)));
    assertInput(TABLE, table, List.of(identity(SCHEMA, schema), identity(CATALOG, catalog)));
    assertInput(
        TABLE, otherTable, List.of(identity(SCHEMA, otherSchema), identity(CATALOG, otherCatalog)));

    hibernateConfigurator
        .getSessionFactory()
        .inTransaction(
            session -> session.get(UserDAO.class, user.getId()).setExternalId("subject-123"));
    assertThat(
            inputBuilder
                .build(user.getId(), TABLE, otherTable.getId(), Privileges.SELECT)
                .at("/input/principal"))
        .isEqualTo(
            JsonUtils.getInstance()
                .valueToTree(
                    Map.of(
                        "id", user.getId().toString(),
                        "email", user.getEmail(),
                        "external_id", "subject-123")));
  }

  @Test
  void rejectsUnresolvedPrincipalResourceOrParent() {
    UUID missingId = UUID.randomUUID();
    assertUnavailable(missingId, CATALOG, catalog.getId());
    for (SecurableType type : SecurableType.values()) {
      assertUnavailable(user.getId(), type, missingId);
    }

    SchemaInfoDAO orphanSchema =
        SchemaInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("orphan_schema")
            .catalogId(missingId)
            .build();
    TableInfoDAO missingSchemaTable =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("missing_schema")
            .schemaId(missingId)
            .build();
    TableInfoDAO missingCatalogTable =
        TableInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("missing_catalog")
            .schemaId(orphanSchema.getId())
            .build();
    persist(orphanSchema, missingSchemaTable, missingCatalogTable);

    assertUnavailable(user.getId(), SCHEMA, orphanSchema.getId());
    assertUnavailable(user.getId(), TABLE, missingSchemaTable.getId());
    assertUnavailable(user.getId(), TABLE, missingCatalogTable.getId());
  }

  private static Map<String, String> identity(SecurableType type, IdentifiableDAO resource) {
    return Map.of(
        "id", resource.getId().toString(), "type", type.name(), "name", resource.getName());
  }

  private void assertInput(
      SecurableType type, IdentifiableDAO resource, List<Map<String, String>> parents) {
    assertInput(
        type,
        resource.getId(),
        Map.of(
            "id", resource.getId().toString(),
            "type", type.name(),
            "name", resource.getName(),
            "parents", parents));
  }

  private void assertInput(SecurableType type, UUID resourceId, Map<String, ?> expectedResource) {
    assertThat(inputBuilder.build(user.getId(), type, resourceId, Privileges.CREATE_TABLE))
        .as("input for %s %s", type, resourceId)
        .isEqualTo(
            JsonUtils.getInstance()
                .valueToTree(
                    Map.of(
                        "input",
                        Map.of(
                            "principal",
                            Map.of("id", user.getId().toString(), "email", user.getEmail()),
                            "privilege",
                            "CREATE_TABLE",
                            "resource",
                            expectedResource))));
  }

  private void assertUnavailable(UUID principalId, SecurableType type, UUID resourceId) {
    assertThatExceptionOfType(BaseException.class)
        .as("metadata for %s %s and principal %s", type, resourceId, principalId)
        .isThrownBy(() -> inputBuilder.build(principalId, type, resourceId, Privileges.SELECT))
        .withMessage("Could not resolve authorization metadata")
        .satisfies(
            exception -> assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE));
  }

  private void persist(Object... resources) {
    hibernateConfigurator
        .getSessionFactory()
        .inTransaction(
            session -> {
              for (Object resource : resources) {
                session.persist(resource);
              }
            });
  }
}
