package io.unitycatalog.server.persist.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import io.unitycatalog.server.model.Privilege;
import org.junit.jupiter.api.Test;

public class PrivilegesTest {

  @Test
  public void testPrivilegesIsSupersetOfPrivilege() {
    // Verify that all generated privileges exist in the persist model
    for (Privilege generatedPrivilege : Privilege.values()) {
      Privileges persistPrivilege = Privileges.fromPrivilege(generatedPrivilege);
      if (persistPrivilege == null) {
        fail(
            String.format(
                "Generated privilege '%s' is not mappable to Privileges enum", generatedPrivilege));
      }
      assertThat(persistPrivilege.name()).isEqualTo(generatedPrivilege.name());
      assertThat(persistPrivilege.getValue()).isEqualTo(generatedPrivilege.getValue());
    }
  }
}
