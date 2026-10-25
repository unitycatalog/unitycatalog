package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.migrate.MigratorTest.create07Schema;
import static io.unitycatalog.server.migrate.MigratorTest.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A database of a release before 0.7.0, after one start of a 0.7.0 server, differs from the 0.7.0
 * schema in known ways, which the tests make by hand on a 0.7.0 database:
 *
 * <ul>
 *   <li>Releases up to 0.4.0 knew 21 function types: the CHECKs on {@code
 *       uc_function_params.type_name} and {@code uc_functions.data_type} allow 0 to 20, not 0 to
 *       21, and their stored types 17 to 20 mean other types now.
 *   <li>Users sharing an external id kept Hibernate from adding {@code uc_users_external_id_key}.
 * </ul>
 */
public class AdoptionRepairTest {

  private static final String WHAT_TO_DO =
      "Fix them as docs/server/upgrades.md shows (section \"Adopting an older database\"), then"
          + " start the server again.";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  void widensTheFunctionTypeChecks() throws SQLException {
    create07Schema(db);
    narrowFunctionTypeChecks();
    insertFunction(1, /* dataType= */ 5, /* paramTypeName= */ 16);

    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    assertThat(SchemaSnapshot.of(db.dataSource()))
        .isEqualTo(RealUpgradeTest.freshInstallSnapshot());
  }

  @Test
  void addsTheMissingExternalIdKey() throws SQLException {
    create07Schema(db);
    dropExternalIdKey();
    insertUser(1, "alice", "ext-alice");
    insertUser(2, "bob", null);
    insertUser(3, "carol", null);

    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    assertThat(SchemaSnapshot.of(db.dataSource()))
        .isEqualTo(RealUpgradeTest.freshInstallSnapshot());
  }

  @Test
  void refusesUsersSharingAnExternalId() throws SQLException {
    create07Schema(db);
    dropExternalIdKey();
    insertUser(1, "alice", "ext-dup");
    insertUser(2, "bob", "ext-dup");
    insertUser(3, "carol", null);
    insertUser(4, "dave", null);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The upgrade is stopped before any change: the database differs from the 0.7.0"
                + " schema, and these rows keep the server from repairing it.\n"
                + "The check for users sharing an external id (decide which user keeps it and"
                + " clear it on the others) found 2 rows:\n"
                + "  id=00000000-0000-0000-0000-000000000001, name=alice, email=alice@example.com,"
                + " external_id=ext-dup\n"
                + "  id=00000000-0000-0000-0000-000000000002, name=bob, email=bob@example.com,"
                + " external_id=ext-dup\n"
                + WHAT_TO_DO);
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);
  }

  @Test
  void refusesFunctionTypesOfReleasesBefore041() throws SQLException {
    create07Schema(db);
    narrowFunctionTypeChecks();
    insertFunction(1, /* dataType= */ 17, /* paramTypeName= */ 20);
    insertFunction(2, /* dataType= */ 5, /* paramTypeName= */ 16);
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The upgrade is stopped before any change: the database differs from the 0.7.0"
                + " schema, and these rows keep the server from repairing it.\n"
                + "The check for function types stored by a release before 0.4.1 (this release"
                + " reads them as other types) found 2 rows:\n"
                + "  function_name=cat.sch.f1, type_column=uc_function_params.type_name,"
                + " parameter_name=p1, stored_value=20\n"
                + "  function_name=cat.sch.f1, type_column=uc_functions.data_type,"
                + " parameter_name=null, stored_value=17\n"
                + WHAT_TO_DO);
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);
  }

  @Test
  void refusesADifferenceItCannotRepair() {
    create07Schema(db);
    execute(db, "ALTER TABLE uc_catalogs ADD COLUMN note VARCHAR(64)");
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource(), db.kind());
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(migrator::migrate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "The database differs from the 0.7.0 schema in ways the server can't repair (-: the"
                + " 0.7.0 schema, +: what the database has):\n"
                + "  +column uc_catalogs.note type=")
        .hasMessageEndingWith(
            "\nCompare it with a fresh 0.7.0 database and fix the differences by hand, or ask on"
                + " the project's issue tracker; see docs/server/upgrades.md (section \"Adopting"
                + " an older database\").");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
    assertThat(migrator.state()).isEqualTo(SchemaState.UNVERSIONED_UC);
  }

  /** The repair is part of the upgrade that the backup undoes. */
  @Test
  void restoresTheChecksWhenTheUpgradeFailsAfterTheRepair() throws SQLException {
    create07Schema(db);
    narrowFunctionTypeChecks();
    SchemaSnapshot before = SchemaSnapshot.of(db.dataSource());
    String failingStep = "classpath:db/test-steps/backup-fail/" + db.kind().folder();
    SchemaMigrator migrator =
        new SchemaMigrator(db.dataSource(), db.kind(), List.of(failingStep)).withBackup(null);

    try {
      assertThatThrownBy(migrator::migrate)
          .hasMessageContaining("V6002__fail_on_a_missing_table.sql")
          .hasMessageEndingWith(
              "The database was restored from the backup taken before the upgrade; the previous"
                  + " release can start on it.");
    } finally {
      BackupTest.dropCompanions(db);
    }
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(before);
  }

  /** Function types 17 to 20 with the 0.7.0 CHECKs: a 0.4.1 or later release stored them. */
  @Test
  void adoptsFunctionTypes17To20UnderThe07Checks() throws SQLException {
    create07Schema(db);
    insertFunction(1, /* dataType= */ 17, /* paramTypeName= */ 20);

    new SchemaMigrator(db.dataSource(), db.kind()).migrate();

    assertThat(SchemaSnapshot.of(db.dataSource()))
        .isEqualTo(RealUpgradeTest.freshInstallSnapshot());
  }

  /** Makes the two function type CHECKs allow 0 to 20, as releases up to 0.4.0 created them. */
  private void narrowFunctionTypeChecks() throws SQLException {
    Map<String, String> names;
    try (Connection connection = db.dataSource().getConnection()) {
      names = SchemaSnapshot.checkNames(connection);
    }
    for (String[] typeColumn :
        new String[][] {{"uc_function_params", "type_name"}, {"uc_functions", "data_type"}}) {
      String table = typeColumn[0];
      String column = typeColumn[1];
      String name =
          names.entrySet().stream()
              .filter(e -> e.getKey().startsWith("check " + table + " "))
              .filter(e -> e.getKey().toLowerCase(Locale.ROOT).contains(column))
              .map(Map.Entry::getValue)
              .findFirst()
              .orElseThrow();
      execute(
          db,
          "ALTER TABLE "
              + table
              + (db.kind() == DatabaseKind.MYSQL ? " DROP CHECK " : " DROP CONSTRAINT ")
              + name);
      execute(db, "ALTER TABLE " + table + " ADD CHECK (" + column + " BETWEEN 0 AND 20)");
    }
  }

  private void dropExternalIdKey() {
    execute(
        db,
        db.kind() == DatabaseKind.MYSQL
            ? "ALTER TABLE uc_users DROP INDEX uc_users_external_id_key"
            : "ALTER TABLE uc_users DROP CONSTRAINT uc_users_external_id_key");
  }

  private void insertUser(int id, String name, String externalId) throws SQLException {
    insert(
        "INSERT INTO uc_users (id, name, email, external_id) VALUES (?, ?, ?, ?)",
        id(id),
        name,
        name + "@example.com",
        externalId);
  }

  /** The function {@code cat.sch.f<n>} with one parameter {@code p<n>}; one catalog and schema. */
  private void insertFunction(int n, int dataType, int paramTypeName) throws SQLException {
    if (n == 1) {
      insert("INSERT INTO uc_catalogs (id, name) VALUES (?, 'cat')", id(100));
      insert(
          "INSERT INTO uc_schemas (id, name, catalog_id) VALUES (?, 'sch', ?)", id(101), id(100));
    }
    insert(
        "INSERT INTO uc_functions (id, name, schema_id, data_type) VALUES (?, ?, ?, ?)",
        id(200 + n),
        "f" + n,
        id(101),
        dataType);
    insert(
        "INSERT INTO uc_function_params (id, name, function_id, input_or_return, type_name,"
            + " type_text) VALUES (?, ?, ?, 0, ?, 'type')",
        id(300 + n),
        "p" + n,
        id(200 + n),
        paramTypeName);
  }

  private void insert(String sql, Object... values) throws SQLException {
    try (Connection connection = db.dataSource().getConnection();
        PreparedStatement insert = connection.prepareStatement(sql)) {
      for (int i = 0; i < values.length; i++) {
        insert.setObject(i + 1, values[i]);
      }
      insert.executeUpdate();
    }
  }

  /** The id {@code 00000000-0000-0000-0000-<n>}, as the database stores ids. */
  private Object id(int n) {
    UUID id = new UUID(0, n);
    return db.kind() == DatabaseKind.MYSQL
        ? ByteBuffer.allocate(16).putLong(0).putLong(n).array()
        : id;
  }
}
