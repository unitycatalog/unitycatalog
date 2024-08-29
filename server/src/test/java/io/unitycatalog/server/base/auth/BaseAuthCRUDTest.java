package io.unitycatalog.server.base.auth;

import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;

public abstract class BaseAuthCRUDTest extends BaseServerTest {

  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;

  @BeforeEach
  public void setUp() {
    System.setProperty("server.authorization", "enable");
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
