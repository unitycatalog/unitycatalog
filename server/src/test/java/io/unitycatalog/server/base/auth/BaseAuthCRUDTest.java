package io.unitycatalog.server.base.auth;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseAuthCRUDTest extends BaseServerTest {

  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("server.authorization", "enable");
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
  }
}
