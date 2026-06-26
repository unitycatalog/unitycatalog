package io.unitycatalog.docker.tests;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.docker.tests.support.AuthSupport;
import io.unitycatalog.docker.tests.support.DockerCompose;
import io.unitycatalog.docker.tests.support.DockerTestConfig;
import io.unitycatalog.docker.tests.support.SparkJdbcClient;
import io.unitycatalog.docker.tests.support.TenantBootstrap;
import io.unitycatalog.docker.tests.support.TenantCleanup;
import io.unitycatalog.docker.tests.support.UcOperations;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

abstract class DockerIntegrationTestBase {

  protected static final String SERVER = DockerTestConfig.SERVER_URL;
  protected static String adminToken;

  @BeforeAll
  static void assumeInfrastructure() throws IOException, InterruptedException {
    adminToken = DockerTestConfig.adminToken();
    Assumptions.assumeTrue(
        DockerTestConfig.isServerReachable(adminToken),
        () -> "UC server not reachable at " + SERVER + " (start docker stack first)");
    UcOperations.waitForServer(SERVER, adminToken);
  }

  protected static String userToken(String email, String password)
      throws IOException, InterruptedException {
    return AuthSupport.ucTokenForUser(SERVER, email, password);
  }

  protected static void bootstrapTenant(TenantBootstrap.TenantSpec spec) throws Exception {
    TenantBootstrap.bootstrap(SERVER, adminToken, spec);
  }

  protected static void deleteTenant(
      String catalog, String externalLocation, String dataExternalLocation, String credential)
      throws Exception {
    if (DockerTestConfig.keepTenants()) {
      return;
    }
    TenantCleanup.deleteTenant(
        SERVER, adminToken, catalog, externalLocation, dataExternalLocation, credential);
  }

  protected static void ensureSpark(String catalog, String token)
      throws IOException, InterruptedException {
    DockerCompose.upSparkStack(catalog, token);
  }

  protected static String runSparkSql(String schema, String sql) throws SQLException {
    return SparkJdbcClient.execute(schema, sql);
  }

  protected static String runSparkSqlAs(String catalog, String schema, String token, String sql)
      throws Exception {
    DockerCompose.upSparkStack(catalog, token);
    return SparkJdbcClient.execute(schema, sql);
  }

  protected static void assertSparkSqlFails(String schema, String sql) throws SQLException {
    try {
      runSparkSql(schema, sql);
      throw new AssertionError("Expected Spark SQL to fail: " + sql);
    } catch (SQLException e) {
      // expected
    }
  }

  protected static void assertRowCount(String output, long expected) {
    // JDBC returns bare "120"; beeline pads with spaces/pipes around values.
    assertThat(output.strip()).contains(Long.toString(expected));
  }

  protected static String fq(String catalog, String schema, String table) {
    return catalog + "." + schema + "." + table;
  }

  protected static TenantBootstrap.TenantSpec tenantSpec(
      String tenantId,
      String catalog,
      String email,
      String credential,
      String el,
      String dataEl) {
    return TenantBootstrap.TenantSpec.builder()
        .tenantId(tenantId)
        .catalogName(catalog)
        .userEmail(email)
        .credentialName(credential)
        .externalLocationName(el)
        .dataExternalLocationName(dataEl)
        .build();
  }
}
