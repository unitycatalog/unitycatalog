package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.CreateCredentialRequest;
import io.unitycatalog.server.model.CredentialPurpose;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

class HibernateConfiguratorTest {

  @Test
  void configuresHikariAndExposesTheSharedDataSource() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.hikari.maximumPoolSize", "3");
    properties.setProperty("hibernate.hikari.minimumIdle", "1");
    properties.setProperty("hibernate.hikari.poolName", "unity-catalog-test");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties)) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(3);
      assertThat(configurator.getDataSource().getMinimumIdle()).isEqualTo(1);
      assertThat(configurator.getDataSource().getPoolName()).isEqualTo("unity-catalog-test");
      assertThat(configurator.getDataSource().isAutoCommit()).isFalse();
      assertThat(configurator.getDataSource().getUsername()).isEqualTo("sa");
      assertThat(configurator.getSessionFactory().isOpen()).isTrue();
    }
  }

  @Test
  void connectionPoolSizeFallsBackToHibernatePoolSize() {
    Properties properties = h2Properties();
    properties.setProperty("hibernate.connection.pool_size", "4");

    try (HibernateConfigurator configurator = new HibernateConfigurator(properties)) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(4);
    }
  }

  @Test
  void connectionPoolSizeDefaultsToTwenty() {
    try (HibernateConfigurator configurator = new HibernateConfigurator(h2Properties())) {
      assertThat(configurator.getDataSource().getMaximumPoolSize()).isEqualTo(20);
    }
  }

  @Test
  void resolvesUsernameFromStandardHibernateProperty() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.username", "alice");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("alice");
  }

  @Test
  void fallsBackToLegacyUserProperty() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.user", "bob");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("bob");
  }

  @Test
  void prefersStandardUsernameWhenBothPresent() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.username", "alice");
    properties.setProperty("hibernate.connection.user", "bob");
    assertThat(HibernateConfigurator.resolveConnectionUsername(properties)).isEqualTo("alice");
  }

  @Test
  void takesThePasswordFromASystemProperty() {
    System.setProperty("hibernate.connection.password", "override-password");
    try {
      Properties properties =
          HibernateConfigurator.setupHibernateProperties(new ServerProperties());
      assertThat(properties.getProperty("hibernate.connection.password"))
          .isEqualTo("override-password");
    } finally {
      System.clearProperty("hibernate.connection.password");
    }
  }

  @Test
  void encryptsStoredCredentialsWithTheConfiguredKey() {
    CredentialCipher cipher = new CredentialCipher(new SecretKeySpec(new byte[32], "AES"));
    try (HibernateConfigurator configurator = new HibernateConfigurator(h2Properties(), cipher)) {
      try (Session session = configurator.getSessionFactory().openSession()) {
        session.beginTransaction();
        session.persist(newCredential("encrypted", "role/r"));
        session.persist(newCredential("plaintext", "role/old"));
        session.flush();
        // A row written before the key was set stays readable.
        session
            .createNativeMutationQuery(
                "UPDATE uc_credentials SET credential = ?1 WHERE name = 'plaintext'")
            .setParameter(1, "{\"role_arn\":\"arn:aws:iam::123456789012:role/old\"}")
            .executeUpdate();
        session.getTransaction().commit();
      }
      try (Session session = configurator.getSessionFactory().openSession()) {
        String stored =
            session
                .createNativeQuery(
                    "SELECT credential FROM uc_credentials WHERE name = 'encrypted'", String.class)
                .getSingleResult();
        assertThat(stored).startsWith(CredentialCipher.PREFIX).doesNotContain("role/r");
        List<String> roleArns =
            session
                .createQuery("FROM CredentialDAO ORDER BY name", CredentialDAO.class)
                .list()
                .stream()
                .map(dao -> dao.getAwsIamRoleResponse().getRoleArn())
                .toList();
        assertThat(roleArns)
            .containsExactly(
                "arn:aws:iam::123456789012:role/r", "arn:aws:iam::123456789012:role/old");
      }
    }
  }

  private static CredentialDAO newCredential(String name, String role) {
    return CredentialDAO.from(
        new CreateCredentialRequest()
            .name(name)
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:" + role)),
        "owner");
  }

  private static Properties h2Properties() {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    properties.setProperty("hibernate.connection.user", "sa");
    properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    return properties;
  }
}
