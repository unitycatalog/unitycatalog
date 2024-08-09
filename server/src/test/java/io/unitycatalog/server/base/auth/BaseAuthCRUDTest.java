package io.unitycatalog.server.base.auth;

import io.unitycatalog.server.base.BaseServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseAuthCRUDTest extends BaseServerTest {

  @BeforeEach
  public void setUp() {
    System.setProperty("server.authorization", "enable");
    super.setUp();
  }

  @AfterEach
  public void cleanUp() {
    System.clearProperty("server.authorization");
  }
}
