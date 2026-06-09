package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.junit.jupiter.api.Test;

/**
 * Runs the {@link PermissionServiceTest} suite with {@code server.authorization=enable}, so the
 * authentication and authorization decorators are enforced.
 */
public class PermissionServiceWithAuthorizationTest extends PermissionServiceTest {

  private static final String CATALOG_NAME = "perm_test_cat";
  private static final String BOB = "bob@localhost";

  @Override
  protected boolean authorizationEnabled() {
    return true;
  }

  @Test
  public void ownerCanGrantAndReadCatalogPermissions() {
    String admin = adminToken();
    createCatalog(CATALOG_NAME, admin);
    createUser(BOB, admin);

    // Admin (metastore + catalog owner) grants USE CATALOG to bob.
    AggregatedHttpResponse grantResponse =
        updateCatalogPermissions(CATALOG_NAME, admin, addPrivilegeBody(BOB, "USE CATALOG"));
    assertThat(grantResponse.status()).isEqualTo(HttpStatus.OK);
    assertThat(privilegesFor(parse(grantResponse), BOB)).containsExactly("USE CATALOG");

    // Reading back as the owner exposes the full ACL, including bob's grant.
    AggregatedHttpResponse getResponse = getCatalogPermissions(CATALOG_NAME, admin);
    assertThat(getResponse.status()).isEqualTo(HttpStatus.OK);
    assertThat(privilegesFor(parse(getResponse), BOB)).containsExactly("USE CATALOG");
  }

  @Test
  public void nonOwnerCannotUpdateCatalogPermissions() {
    String admin = adminToken();
    createCatalog(CATALOG_NAME, admin);
    createUser(BOB, admin);

    // Bob is neither metastore nor catalog owner, so the @AuthorizeExpression rejects the update.
    AggregatedHttpResponse response =
        updateCatalogPermissions(
            CATALOG_NAME, userToken(BOB), addPrivilegeBody(BOB, "USE CATALOG"));
    assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  public void nonOwnerCannotReadCatalogPermissions() {
    String admin = adminToken();
    createCatalog(CATALOG_NAME, admin);
    createUser(BOB, admin);

    // GET is now gated to owners, so a non-owner read is rejected by the @AuthorizeExpression.
    AggregatedHttpResponse response = getCatalogPermissions(CATALOG_NAME, userToken(BOB));
    assertThat(response.status()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  public void unauthenticatedRequestIsRejected() {
    String admin = adminToken();
    createCatalog(CATALOG_NAME, admin);

    // No bearer token -> the AuthDecorator rejects the request before it reaches the service.
    AggregatedHttpResponse response = getCatalogPermissions(CATALOG_NAME, null);
    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }
}
