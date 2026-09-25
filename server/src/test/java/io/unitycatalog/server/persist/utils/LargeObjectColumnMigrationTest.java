package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.cfg.MappingSettings;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Upgrades a PostgreSQL database whose columns were written by the earlier {@code @Lob} mapping.
 */
@Testcontainers(disabledWithoutDocker = true)
class LargeObjectColumnMigrationTest {

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("unitycatalog_test")
          .withUsername("test")
          .withPassword("test");

  private static final String TYPE_TEXT = "struct<straße:string,note:string>" + "x".repeat(100_000);
  private static final String VIEW_DEFINITION = "SELECT * FROM t WHERE city = 'Zürich'";
  private static final String ROUTINE_DEFINITION = "RETURN concat(x, 'é')";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/unity";

  /** Table and column names of what the earlier mapping stored as large objects. */
  private static final List<List<String>> LARGE_OBJECT_COLUMNS =
      List.of(
          List.of("uc_tables", "view_definition"),
          List.of("uc_columns", "type_text"),
          List.of("uc_functions", "routine_definition"),
          List.of("uc_credentials", "credential"));

  @Test
  void convertsLargeObjectColumnsBeforeTheSchemaUpdate() throws SQLException {
    upgradeKeepsValues("public", new Properties());
  }

  @Test
  void convertsColumnsInAQuotedDefaultSchema() throws SQLException {
    // Hibernate quotes the default schema as well, so PostgreSQL keeps its case.
    execute("create schema \"MixedCase\"");
    Properties settings = new Properties();
    settings.setProperty(MappingSettings.DEFAULT_SCHEMA, "MixedCase");
    settings.setProperty(MappingSettings.GLOBALLY_QUOTED_IDENTIFIERS, "true");
    upgradeKeepsValues("MixedCase", settings);
  }

  private static void upgradeKeepsValues(String schema, Properties settings) throws SQLException {
    UUID viewId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    UUID functionId = UUID.randomUUID();
    CredentialDAO credential =
        CredentialDAO.from(
            new CreateCredentialRequest()
                .name("c")
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(new AwsIamRoleRequest().roleArn(ROLE_ARN)),
            "owner");
    UUID credentialId = credential.getId();
    try (HibernateConfigurator configurator =
        new HibernateConfigurator(properties("create", settings))) {
      configurator
          .getSessionFactory()
          .inTransaction(
              session -> {
                session.persist(table(viewId, "v", VIEW_DEFINITION));
                session.persist(table(tableId, "t", null));
                session.persist(
                    FunctionInfoDAO.builder()
                        .id(functionId)
                        .name("f")
                        .routineDefinition(ROUTINE_DEFINITION)
                        .build());
                session.persist(credential);
              });
    }
    storeAsLargeObjects(schema);
    assertDataTypes(schema, "oid");

    try (HibernateConfigurator configurator =
        new HibernateConfigurator(properties("update", settings))) {
      assertDataTypes(schema, "text");
      assertRowsIntact(configurator, viewId, tableId, functionId, credentialId);
    }

    // Later starts find text columns and leave them as they are.
    try (HibernateConfigurator configurator =
        new HibernateConfigurator(properties("update", settings))) {
      assertRowsIntact(configurator, viewId, tableId, functionId, credentialId);
    }
  }

  private static void assertRowsIntact(
      HibernateConfigurator configurator,
      UUID viewId,
      UUID tableId,
      UUID functionId,
      UUID credentialId) {
    configurator
        .getSessionFactory()
        .inSession(
            session -> {
              TableInfoDAO view = session.get(TableInfoDAO.class, viewId);
              assertThat(view.getViewDefinition()).isEqualTo(VIEW_DEFINITION);
              assertThat(view.getColumns())
                  .extracting(ColumnInfoDAO::getTypeText)
                  .containsExactly(TYPE_TEXT);
              assertThat(session.get(TableInfoDAO.class, tableId).getViewDefinition()).isNull();
              assertThat(session.get(FunctionInfoDAO.class, functionId).getRoutineDefinition())
                  .isEqualTo(ROUTINE_DEFINITION);
              assertThat(
                      session
                          .get(CredentialDAO.class, credentialId)
                          .toCredentialInfo(Optional.empty())
                          .getAwsIamRole()
                          .getRoleArn())
                  .isEqualTo(ROLE_ARN);
            });
  }

  private static TableInfoDAO table(UUID id, String name, String viewDefinition) {
    TableInfoDAO table =
        TableInfoDAO.builder()
            .id(id)
            .name(name)
            .type(viewDefinition != null ? "VIEW" : "MANAGED")
            .viewDefinition(viewDefinition)
            .build();
    table.setColumns(
        List.of(
            ColumnInfoDAO.builder()
                .id(UUID.randomUUID())
                .name("c")
                .table(table)
                .ordinalPosition((short) 0)
                .typeText(TYPE_TEXT)
                .typeJson("{}")
                .typeName("STRUCT")
                .build()));
    return table;
  }

  /** Puts the columns back the way the {@code @Lob} mapping stored them: one large object each. */
  private static void storeAsLargeObjects(String schema) throws SQLException {
    for (List<String> column : LARGE_OBJECT_COLUMNS) {
      execute(
          String.format(
              "alter table \"%s\".%s alter column %3$s type oid"
                  + " using lo_from_bytea(0, convert_to(%3$s, 'UTF8'))",
              schema, column.get(0), column.get(1)));
    }
  }

  private static void assertDataTypes(String schema, String dataType) throws SQLException {
    try (Connection connection = connect();
        PreparedStatement statement =
            connection.prepareStatement(
                "select data_type from information_schema.columns"
                    + " where table_schema = ? and table_name = ? and column_name = ?")) {
      for (List<String> column : LARGE_OBJECT_COLUMNS) {
        String name = column.get(0) + "." + column.get(1);
        statement.setString(1, schema);
        statement.setString(2, column.get(0));
        statement.setString(3, column.get(1));
        try (ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).as(name).isTrue();
          assertThat(resultSet.getString(1)).as(name).isEqualTo(dataType);
        }
      }
    }
  }

  private static void execute(String sql) throws SQLException {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static Properties properties(String hbm2ddl, Properties settings) {
    Properties properties = new Properties();
    properties.putAll(settings);
    properties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    properties.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    properties.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    properties.setProperty("hibernate.connection.password", POSTGRES.getPassword());
    properties.setProperty("hibernate.hbm2ddl.auto", hbm2ddl);
    return properties;
  }
}
