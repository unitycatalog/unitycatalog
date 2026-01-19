package io.unitycatalog.server.base.access;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Path;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseAccessControlCRUDTest extends BaseCRUDTest {

  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();

    Path configurationFolder = Path.of("etc", "conf");

    securityConfiguration = new SecurityConfiguration(configurationFolder);
    securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);
  }

  @AfterEach
  public void cleanUp() {
    System.clearProperty("server.authorization");

    SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
    Session session = sessionFactory.openSession();
    Transaction tx = session.beginTransaction();
    session.createNativeMutationQuery("delete from casbin_rule").executeUpdate();
    tx.commit();
    session.close();

    super.tearDown();
  }
}
