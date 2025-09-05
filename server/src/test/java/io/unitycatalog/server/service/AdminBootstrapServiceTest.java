package io.unitycatalog.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.control.model.BootstrapOwnerRequest;
import io.unitycatalog.control.model.BootstrapOwnerResponse;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.BootstrapStateRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.BootstrapState;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.jwt.JwksOperations;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AdminBootstrapServiceTest extends BaseServerTest {

  private static final String BOOTSTRAP_ENDPOINT = "/api/2.1/unity-catalog/admins/bootstrap-owner";
  private WebClient client;

  @Mock private JwksOperations jwksOperations;
  @Mock private BootstrapStateRepository bootstrapStateRepository;
  @Mock private UnityCatalogAuthorizer authorizer;
  @Mock private Repositories repositories;
  @Mock private io.unitycatalog.server.persist.UserRepository userRepository;

  private AdminBootstrapService bootstrapService;
  private DecodedJWT mockJwt;
  private Claim mockClaim;

  @BeforeEach
  public void setUp() {
    super.setUp();
    MockitoAnnotations.openMocks(this);

    String uri = serverConfig.getServerUrl();
    String token = serverConfig.getAuthToken();
    client = WebClient.builder(uri).auth(AuthToken.ofOAuth2(token)).build();

    // Setup mocks
    mockJwt = mock(DecodedJWT.class);
    mockClaim = mock(Claim.class);

    when(repositories.getBootstrapStateRepository()).thenReturn(bootstrapStateRepository);
    when(repositories.getUserRepository()).thenReturn(userRepository);

    bootstrapService = new AdminBootstrapService(authorizer, repositories, jwksOperations);
  }

  @Test
  public void testBootstrapOwnerSuccess() {
    // Setup Azure JWT validation
    String azureObjectId = "test-azure-oid-123";
    String principalEmail = "test@contoso.com";
    String displayName = "Test User";
    String metastoreId = UUID.randomUUID().toString();

    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(azureObjectId);
    when(mockJwt.getClaim("preferred_username")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(principalEmail, displayName);
    when(mockJwt.getClaim("name")).thenReturn(mockClaim);

    // Setup bootstrap window open
    when(bootstrapStateRepository.isBootstrapWindowOpen()).thenReturn(true);
    when(bootstrapStateRepository.findByMetastoreId(metastoreId)).thenReturn(Optional.empty());

    BootstrapState mockBootstrapState = new BootstrapState();
    mockBootstrapState.setMetastoreId(metastoreId);
    mockBootstrapState.setAzureObjectId(azureObjectId);
    mockBootstrapState.setPrincipalEmail(principalEmail);
    mockBootstrapState.setUserId("user-123");

    when(bootstrapStateRepository.create(any(), any(), any(), any()))
        .thenReturn(mockBootstrapState);

    // Execute bootstrap
    BootstrapOwnerRequest request = new BootstrapOwnerRequest().metastoreId(metastoreId);

    BootstrapOwnerResponse response =
        bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);

    // Verify response
    assertNotNull(response);
    assertEquals(metastoreId, response.getMetastoreId());
    assertEquals(principalEmail, response.getPrincipalEmail());
    assertNotNull(response.getPatToken());
    assertTrue(response.getPatToken().startsWith("pat_"));
  }

  @Test
  public void testBootstrapOwnerIdempotent() {
    // Setup existing bootstrap state
    String azureObjectId = "test-azure-oid-123";
    String principalEmail = "test@contoso.com";
    String displayName = "Test User";
    String metastoreId = UUID.randomUUID().toString();

    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(azureObjectId);
    when(mockJwt.getClaim("preferred_username")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(principalEmail, displayName);
    when(mockJwt.getClaim("name")).thenReturn(mockClaim);

    BootstrapState existingBootstrap = new BootstrapState();
    existingBootstrap.setAzureObjectId(azureObjectId);
    existingBootstrap.setMetastoreId(metastoreId);
    existingBootstrap.setPrincipalEmail(principalEmail);
    existingBootstrap.setUserId("existing-user-123");

    when(bootstrapStateRepository.findByMetastoreId(metastoreId))
        .thenReturn(Optional.of(existingBootstrap));

    // Execute bootstrap
    BootstrapOwnerRequest request = new BootstrapOwnerRequest().metastoreId(metastoreId);

    BootstrapOwnerResponse response =
        bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);

    // Verify idempotent response
    assertNotNull(response);
    assertEquals(metastoreId, response.getMetastoreId());
    assertEquals(principalEmail, response.getPrincipalEmail());
    assertEquals("existing-user-123", response.getUserId());
    assertEquals("[HIDDEN - Already Issued]", response.getPatToken());
    assertTrue(response.getMessage().contains("already bootstrapped"));
  }

  @Test
  public void testBootstrapOwnerConflict() {
    // Setup different Azure principal attempting bootstrap
    String azureObjectId = "different-azure-oid-456";
    String principalEmail = "different@contoso.com";
    String displayName = "Different User";
    String metastoreId = UUID.randomUUID().toString();

    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(azureObjectId);
    when(mockJwt.getClaim("preferred_username")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(principalEmail, displayName);
    when(mockJwt.getClaim("name")).thenReturn(mockClaim);

    BootstrapState existingBootstrap = new BootstrapState();
    existingBootstrap.setAzureObjectId("original-azure-oid-123");
    existingBootstrap.setMetastoreId(metastoreId);

    when(bootstrapStateRepository.findByMetastoreId(metastoreId))
        .thenReturn(Optional.of(existingBootstrap));

    // Execute bootstrap - should conflict
    BootstrapOwnerRequest request = new BootstrapOwnerRequest().metastoreId(metastoreId);

    BaseException exception =
        assertThrows(
            BaseException.class,
            () -> {
              bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);
            });

    assertEquals(ErrorCode.ALREADY_EXISTS, exception.getErrorCode());
    assertTrue(exception.getMessage().contains("OWNER already exists"));
  }

  @Test
  public void testBootstrapOwnerWindowClosed() {
    // Setup Azure JWT validation
    String azureObjectId = "test-azure-oid-123";
    String principalEmail = "test@contoso.com";
    String displayName = "Test User";
    String metastoreId = UUID.randomUUID().toString();

    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(azureObjectId);
    when(mockJwt.getClaim("preferred_username")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(principalEmail, displayName);
    when(mockJwt.getClaim("name")).thenReturn(mockClaim);

    // Setup bootstrap window closed and no existing privileges
    when(bootstrapStateRepository.isBootstrapWindowOpen()).thenReturn(false);
    when(bootstrapStateRepository.findByMetastoreId(metastoreId)).thenReturn(Optional.empty());
    when(authorizer.authorize(any(), any(), any())).thenReturn(false);

    // Execute bootstrap - should fail
    BootstrapOwnerRequest request = new BootstrapOwnerRequest().metastoreId(metastoreId);

    BaseException exception =
        assertThrows(
            BaseException.class,
            () -> {
              bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);
            });

    assertEquals(ErrorCode.PERMISSION_DENIED, exception.getErrorCode());
    assertTrue(exception.getMessage().contains("Bootstrap window closed"));
  }

  @Test
  public void testBootstrapOwnerWithBootstrapPrivilege() {
    // Setup Azure JWT validation
    String azureObjectId = "test-azure-oid-123";
    String principalEmail = "test@contoso.com";
    String displayName = "Test User";
    String metastoreId = UUID.randomUUID().toString();

    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(azureObjectId);
    when(mockJwt.getClaim("preferred_username")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(principalEmail, displayName);
    when(mockJwt.getClaim("name")).thenReturn(mockClaim);

    // Setup BOOTSTRAP_OWNER privilege authorization
    UUID principalId = UUID.randomUUID();
    when(userRepository.findPrincipalId()).thenReturn(principalId);
    when(authorizer.authorize(principalId, UUID.fromString(metastoreId), Privileges.OWNER))
        .thenReturn(false);
    when(authorizer.authorize(
            principalId, UUID.fromString(metastoreId), Privileges.BOOTSTRAP_OWNER))
        .thenReturn(true);
    when(bootstrapStateRepository.findByMetastoreId(metastoreId)).thenReturn(Optional.empty());

    BootstrapState mockBootstrapState = new BootstrapState();
    mockBootstrapState.setMetastoreId(metastoreId);
    mockBootstrapState.setAzureObjectId(azureObjectId);
    mockBootstrapState.setPrincipalEmail(principalEmail);
    mockBootstrapState.setUserId("user-123");

    when(bootstrapStateRepository.create(any(), any(), any(), any()))
        .thenReturn(mockBootstrapState);

    // Execute bootstrap
    BootstrapOwnerRequest request = new BootstrapOwnerRequest().metastoreId(metastoreId);

    BootstrapOwnerResponse response =
        bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);

    // Verify response
    assertNotNull(response);
    assertEquals(metastoreId, response.getMetastoreId());
    assertEquals(principalEmail, response.getPrincipalEmail());
    assertNotNull(response.getPatToken());
  }

  @Test
  public void testInvalidBearerToken() {
    BootstrapOwnerRequest request =
        new BootstrapOwnerRequest().metastoreId(UUID.randomUUID().toString());

    BaseException exception =
        assertThrows(
            BaseException.class,
            () -> {
              bootstrapService.bootstrapOwner("Invalid authorization", request);
            });

    assertEquals(ErrorCode.UNAUTHENTICATED, exception.getErrorCode());
    assertTrue(exception.getMessage().contains("Bearer token required"));
  }

  @Test
  public void testMissingJwtClaims() {
    when(jwksOperations.validateAzureJwt(anyString())).thenReturn(mockJwt);
    when(mockJwt.getClaim("oid")).thenReturn(mockClaim);
    when(mockClaim.asString()).thenReturn(null); // Missing oid claim

    BootstrapOwnerRequest request =
        new BootstrapOwnerRequest().metastoreId(UUID.randomUUID().toString());

    BaseException exception =
        assertThrows(
            BaseException.class,
            () -> {
              bootstrapService.bootstrapOwner("Bearer valid-azure-jwt", request);
            });

    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
    assertTrue(exception.getMessage().contains("Missing required Azure JWT claims"));
  }
}
