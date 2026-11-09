package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.SCHEMA_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.PermissionsList;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.PrivilegeAssignment;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class PermissionServiceLegacyGrantTest extends SdkAccessControlBaseCRUDTest {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STRICT_PRIVILEGE_CHECKS_ENABLED.getKey(), "false");
  }

  @Test
  @SneakyThrows
  public void invalidGrantsAreSavedAndCanBeRemoved() {
    String firstUserId = createTestUser(REGULAR_1).getId();
    String secondUserId = createTestUser(REGULAR_2).getId();
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);

    UpdatePermissions request =
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .add(List.of(Privilege.CREATE_TABLE))
                        .remove(List.of(Privilege.USE_SCHEMA)),
                    new PermissionsChange()
                        .principal(REGULAR_2)
                        .add(List.of(Privilege.USE_SCHEMA, Privilege.USE_CATALOG))
                        .remove(List.of())));
    GrantsApi nonOwnerGrants =
        new GrantsApi(TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1)));
    assertPermissionDenied(
        () -> nonOwnerGrants.update(SecurableType.SCHEMA, SCHEMA_FULL_NAME, request));

    grantsApi.update(SecurableType.SCHEMA, SCHEMA_FULL_NAME, request);

    PermissionsList permissions = grantsApi.get(SecurableType.SCHEMA, SCHEMA_FULL_NAME, null);
    assertThat(permissions.getPrivilegeAssignments())
        .filteredOn(assignment -> REGULAR_1.equals(assignment.getPrincipal()))
        .flatExtracting(PrivilegeAssignment::getPrivileges)
        .containsExactly(Privilege.CREATE_TABLE);
    assertThat(permissions.getPrivilegeAssignments())
        .filteredOn(assignment -> REGULAR_2.equals(assignment.getPrincipal()))
        .flatExtracting(PrivilegeAssignment::getPrivileges)
        .containsExactlyInAnyOrder(Privilege.USE_SCHEMA, Privilege.USE_CATALOG);
    assertThat(countCasbinRulesReferencing(firstUserId)).isOne();
    assertThat(countCasbinRulesReferencing(secondUserId)).isEqualTo(2);

    grantsApi.update(
        SecurableType.SCHEMA,
        SCHEMA_FULL_NAME,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_2)
                        .remove(List.of(Privilege.USE_CATALOG)))));

    assertThat(
            grantsApi.get(SecurableType.SCHEMA, SCHEMA_FULL_NAME, null).getPrivilegeAssignments())
        .filteredOn(assignment -> REGULAR_2.equals(assignment.getPrincipal()))
        .flatExtracting(PrivilegeAssignment::getPrivileges)
        .containsExactly(Privilege.USE_SCHEMA);
    assertThat(countCasbinRulesReferencing(secondUserId)).isOne();
  }
}
