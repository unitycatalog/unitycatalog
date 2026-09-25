package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.LargeObjectColumnMigration.TableColumn;
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

  @Test
  void convertsLargeObjectColumnsBeforeTheSchemaUpdate() throws SQLException {
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
    try (HibernateConfigurator configurator = new HibernateConfigurator(properties("create"))) {
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
    storeAsLargeObjects();
    assertThat(largeObjectColumns()).hasSize(LargeObjectColumnMigration.COLUMNS.size());

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties("update"))) {
      assertThat(largeObjectColumns()).isEmpty();
      assertThat(dataType("uc_columns", "type_text")).isEqualTo("text");
      assertThat(dataType("uc_tables", "view_definition")).isEqualTo("text");
      assertThat(dataType("uc_functions", "routine_definition")).isEqualTo("text");
      assertThat(dataType("uc_credentials", "credential")).isEqualTo("text");
      assertRowsIntact(configurator, viewId, tableId, functionId, credentialId);
    }

    // Later starts find text columns and leave them as they are.
    try (HibernateConfigurator configurator = new HibernateConfigurator(properties("update"))) {
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
  private static void storeAsLargeObjects() throws SQLException {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      for (TableColumn column : LargeObjectColumnMigration.COLUMNS) {
        statement.execute(
            String.format(
                "alter table %s alter column %2$s type oid"
                    + " using lo_from_bytea(0, convert_to(%2$s, 'UTF8'))",
                column.table(), column.column()));
      }
    }
  }

  private static List<TableColumn> largeObjectColumns() throws SQLException {
    try (Connection connection = connect()) {
      return LargeObjectColumnMigration.largeObjectColumns(connection, null);
    }
  }

  private static String dataType(String table, String column) throws SQLException {
    try (Connection connection = connect();
        PreparedStatement statement =
            connection.prepareStatement(
                "select data_type from information_schema.columns"
                    + " where table_name = ? and column_name = ?")) {
      statement.setString(1, table);
      statement.setString(2, column);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        return resultSet.getString(1);
      }
    }
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static Properties properties(String hbm2ddl) {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    properties.setProperty("hibernate.connection.url", POSTGRES.getJdbcUrl());
    properties.setProperty("hibernate.connection.username", POSTGRES.getUsername());
    properties.setProperty("hibernate.connection.password", POSTGRES.getPassword());
    properties.setProperty("hibernate.hbm2ddl.auto", hbm2ddl);
    return properties;
  }
}
