package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.junit.jupiter.api.Test;

/**
 * Runs the {@link PermissionServiceTest} suite with authorization disabled (the server default). No
 * authentication/authorization decorators are installed and the server uses an {@code
 * AllowingAuthorizer}, so the endpoints are reachable without a token and grants are no-ops.
 */
public class PermissionServiceWithoutAuthorizationTest extends PermissionServiceTest {

  private static final String CATALOG_NAME = "perm_test_cat_noauth";
  private static final String BOB = "bob@localhost";

  @Override
  protected boolean authorizationEnabled() {
    return false;
  }

  @Test
  public void permissionEndpointsAreReachableWithoutAuthentication() {
    // No token is required when authorization is disabled.
    createCatalog(CATALOG_NAME, null);

    AggregatedHttpResponse getResponse = getCatalogPermissions(CATALOG_NAME, null);
    assertThat(getResponse.status()).isEqualTo(HttpStatus.OK);
    // The AllowingAuthorizer does not persist any grants, so the ACL is empty.
    assertThat(parse(getResponse).get("privilege_assignments")).isEmpty();
  }

  @Test
  public void updatePermissionsSucceedsButIsANoOp() {
    createCatalog(CATALOG_NAME, null);
    createUser(BOB, null);

    AggregatedHttpResponse response =
        updateCatalogPermissions(CATALOG_NAME, null, addPrivilegeBody(BOB, "USE CATALOG"));

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    // Grants are no-ops with the AllowingAuthorizer, so nothing is reflected back.
    assertThat(privilegesFor(parse(response), BOB)).isEmpty();
  }
}
