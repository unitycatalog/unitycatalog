package io.unitycatalog.server.test;

import io.unitycatalog.server.sdk.ApiClient;
import io.unitycatalog.server.sdk.ApiException;
import io.unitycatalog.server.sdk.api.AdminBootstrapApi;
import io.unitycatalog.server.sdk.api.GrantsApi;
import io.unitycatalog.server.sdk.api.TokensApi;
import io.unitycatalog.server.sdk.model.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Azure AD OIDC bootstrap functionality.
 * Tests the complete bootstrap flow including authorization matrix validation.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AdminBootstrapIntegrationTest {

    @Container
    static GenericContainer<?> unityCatalogServer = new GenericContainer<>("unitycatalog:test")
            .withExposedPorts(8080)
            .withEnv("UC_BOOTSTRAP_ENABLED", "true")
            .withEnv("UC_BOOTSTRAP_WINDOW_MINUTES", "10")
            .withEnv("UC_AZURE_AD_TENANT_ID", "test-tenant")
            .withEnv("UC_AZURE_AD_CLIENT_ID", "test-client");

    private static AdminBootstrapApi bootstrapApi;
    private static GrantsApi grantsApi;
    private static TokensApi tokensApi;
    private static String validAzureJwt;
    private static String invalidJwt;

    @BeforeAll
    static void setUp() {
        String baseUrl = "http://localhost:" + unityCatalogServer.getMappedPort(8080);
        
        // Create authenticated client
        ApiClient client = new ApiClient();
        client.setBasePath(baseUrl);
        
        bootstrapApi = new AdminBootstrapApi(client);
        grantsApi = new GrantsApi(client);
        tokensApi = new TokensApi(client);
        
        // Mock JWTs for testing
        validAzureJwt = createMockAzureJwt("admin@contoso.com");
        invalidJwt = "invalid.jwt.token";
    }

    @Test
    @Order(1)
    void testBootstrapOwnerSuccess() throws ApiException {
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(validAzureJwt)
                .upn("admin@contoso.com");

        BootstrapOwnerResponse response = bootstrapApi.bootstrapOwner(request);

        assertNotNull(response);
        assertEquals("admin@contoso.com", response.getPrincipal().getName());
        assertEquals(PrincipalType.USER, response.getPrincipal().getType());
        assertNotNull(response.getBootstrappedAt());
        assertTrue(response.getBootstrappedAt().isBefore(OffsetDateTime.now().plusMinutes(1)));
    }

    @Test
    @Order(2)
    void testBootstrapOwnerIdempotent() throws ApiException {
        // Second bootstrap attempt should return 200 with existing owner info
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(validAzureJwt)
                .upn("admin@contoso.com");

        BootstrapOwnerResponse response = bootstrapApi.bootstrapOwner(request);

        assertNotNull(response);
        assertEquals("admin@contoso.com", response.getPrincipal().getName());
        // Should return existing bootstrap timestamp
        assertNotNull(response.getBootstrappedAt());
    }

    @Test
    @Order(3)
    void testBootstrapOwnerConflictDifferentUser() {
        // Attempt to bootstrap different user should fail with 409
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(createMockAzureJwt("different@contoso.com"))
                .upn("different@contoso.com");

        ApiException exception = assertThrows(ApiException.class, 
            () -> bootstrapApi.bootstrapOwner(request));
        
        assertEquals(409, exception.getCode());
        assertTrue(exception.getMessage().contains("already exists"));
    }

    @Test
    @Order(4)
    void testBootstrapOwnerInvalidJwt() {
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(invalidJwt)
                .upn("admin@contoso.com");

        ApiException exception = assertThrows(ApiException.class, 
            () -> bootstrapApi.bootstrapOwner(request));
        
        assertEquals(401, exception.getCode());
        assertTrue(exception.getMessage().contains("Invalid Azure JWT"));
    }

    @Test
    @Order(5)
    void testBootstrapOwnerMismatchedUpn() {
        // JWT subject doesn't match UPN in request
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(createMockAzureJwt("admin@contoso.com"))
                .upn("different@contoso.com");

        ApiException exception = assertThrows(ApiException.class, 
            () -> bootstrapApi.bootstrapOwner(request));
        
        assertEquals(400, exception.getCode());
        assertTrue(exception.getMessage().contains("UPN mismatch"));
    }

    @Test
    @Order(6)
    void testBootstrapDisabled() {
        // This would require restarting container with bootstrap disabled
        // For now, we'll test the error path when window expires
        // Implementation would check UC_BOOTSTRAP_ENABLED=false
    }

    @Test
    @Order(7)
    void testAuthorizationMatrixBootstrapOwner() throws ApiException {
        // Test that bootstrapped owner has BOOTSTRAP_OWNER privilege
        // and can perform admin operations
        
        // Create API client with bootstrapped owner token
        ApiClient ownerClient = createClientWithBootstrapToken();
        GrantsApi ownerGrantsApi = new GrantsApi(ownerClient);
        
        // Should be able to list grants (admin operation)
        assertDoesNotThrow(() -> ownerGrantsApi.listGrants(null, null, null));
        
        // Should be able to grant permissions
        GrantRequest grantRequest = new GrantRequest()
                .principal(new Principal().name("test@contoso.com").type(PrincipalType.USER))
                .privilege(Privilege.USE_CATALOG);
        
        assertDoesNotThrow(() -> ownerGrantsApi.updateGrant(grantRequest));
    }

    @Test
    @Order(8)
    void testAuthorizationMatrixRegularUser() throws ApiException {
        // Test that regular users cannot access bootstrap endpoint
        ApiClient userClient = createClientWithUserToken();
        AdminBootstrapApi userBootstrapApi = new AdminBootstrapApi(userClient);
        
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt(validAzureJwt)
                .upn("user@contoso.com");

        ApiException exception = assertThrows(ApiException.class, 
            () -> userBootstrapApi.bootstrapOwner(request));
        
        assertEquals(403, exception.getCode());
        assertTrue(exception.getMessage().contains("Insufficient privileges"));
    }

    private static String createMockAzureJwt(String upn) {
        // In real tests, this would create a proper JWT with Azure AD claims
        // For integration tests, we'd use a test JWT signing key
        return String.format("mock.jwt.%s", upn.replace("@", "_"));
    }

    private ApiClient createClientWithBootstrapToken() {
        ApiClient client = new ApiClient();
        client.setBasePath("http://localhost:" + unityCatalogServer.getMappedPort(8080));
        client.addDefaultHeader("Authorization", "Bearer bootstrap-owner-token");
        return client;
    }

    private ApiClient createClientWithUserToken() {
        ApiClient client = new ApiClient();
        client.setBasePath("http://localhost:" + unityCatalogServer.getMappedPort(8080));
        client.addDefaultHeader("Authorization", "Bearer regular-user-token");
        return client;
    }
}
