package io.unitycatalog.server.test;

import io.unitycatalog.server.sdk.ApiClient;
import io.unitycatalog.server.sdk.ApiException;
import io.unitycatalog.server.sdk.api.TokensApi;
import io.unitycatalog.server.sdk.model.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Personal Access Token (PAT) management functionality.
 * Tests token creation, listing, revocation, and security properties.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PersonalAccessTokenIntegrationTest {

    @Container
    static GenericContainer<?> unityCatalogServer = new GenericContainer<>("unitycatalog:test")
            .withExposedPorts(8080)
            .withEnv("UC_PAT_ENABLED", "true")
            .withEnv("UC_PAT_DEFAULT_TTL_MINUTES", "60");

    private static TokensApi ownerTokensApi;
    private static TokensApi userTokensApi;
    private static String createdTokenId;
    private static String createdTokenValue;

    @BeforeAll
    static void setUp() {
        String baseUrl = "http://localhost:" + unityCatalogServer.getMappedPort(8080);
        
        // Create API clients for different privilege levels
        ApiClient ownerClient = new ApiClient();
        ownerClient.setBasePath(baseUrl);
        ownerClient.addDefaultHeader("Authorization", "Bearer owner-token");
        ownerTokensApi = new TokensApi(ownerClient);
        
        ApiClient userClient = new ApiClient();
        userClient.setBasePath(baseUrl);
        userClient.addDefaultHeader("Authorization", "Bearer user-token");
        userTokensApi = new TokensApi(userClient);
    }

    @Test
    @Order(1)
    void testCreatePersonalAccessToken() throws ApiException {
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Test token for integration tests")
                .lifetimeSeconds(3600L); // 1 hour

        CreateTokenResponse response = ownerTokensApi.createToken(request);

        assertNotNull(response);
        assertNotNull(response.getTokenId());
        assertNotNull(response.getTokenValue());
        assertEquals("Test token for integration tests", response.getComment());
        assertNotNull(response.getCreatedAt());
        assertNotNull(response.getExpiresAt());
        
        // Token value should be displayed once
        assertTrue(response.getTokenValue().length() > 20);
        
        // Store for subsequent tests
        createdTokenId = response.getTokenId();
        createdTokenValue = response.getTokenValue();
    }

    @Test
    @Order(2)
    void testCreateTokenDefaultTtl() throws ApiException {
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Test token with default TTL");
        // No lifetimeSeconds specified - should use default

        CreateTokenResponse response = ownerTokensApi.createToken(request);

        assertNotNull(response);
        assertNotNull(response.getExpiresAt());
        
        // Should expire in approximately 1 hour (default TTL)
        long expectedExpiry = System.currentTimeMillis() + (60 * 60 * 1000); // 1 hour
        long actualExpiry = response.getExpiresAt().toInstant().toEpochMilli();
        assertTrue(Math.abs(actualExpiry - expectedExpiry) < 60000); // Within 1 minute
    }

    @Test
    @Order(3)
    void testListPersonalAccessTokens() throws ApiException {
        ListTokensResponse response = ownerTokensApi.listTokens();

        assertNotNull(response);
        assertNotNull(response.getTokens());
        assertTrue(response.getTokens().size() >= 2); // At least the tokens we created
        
        // Find our created token
        TokenInfo createdToken = response.getTokens().stream()
                .filter(token -> createdTokenId.equals(token.getTokenId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Created token not found in list"));
        
        assertEquals("Test token for integration tests", createdToken.getComment());
        assertNotNull(createdToken.getCreatedAt());
        assertNotNull(createdToken.getExpiresAt());
        assertEquals(TokenStatus.ACTIVE, createdToken.getStatus());
        
        // Token value should NOT be returned in list
        assertNull(createdToken.getTokenValue());
    }

    @Test
    @Order(4)
    void testTokenAuthentication() throws ApiException {
        // Create a new API client using the PAT token
        ApiClient patClient = new ApiClient();
        patClient.setBasePath("http://localhost:" + unityCatalogServer.getMappedPort(8080));
        patClient.addDefaultHeader("Authorization", "Bearer " + createdTokenValue);
        
        TokensApi patTokensApi = new TokensApi(patClient);
        
        // Should be able to list tokens with PAT authentication
        assertDoesNotThrow(() -> patTokensApi.listTokens());
    }

    @Test
    @Order(5)
    void testRevokePersonalAccessToken() throws ApiException {
        ownerTokensApi.revokeToken(createdTokenId);
        
        // Verify token is revoked
        ListTokensResponse response = ownerTokensApi.listTokens();
        TokenInfo revokedToken = response.getTokens().stream()
                .filter(token -> createdTokenId.equals(token.getTokenId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Revoked token not found"));
        
        assertEquals(TokenStatus.REVOKED, revokedToken.getStatus());
    }

    @Test
    @Order(6)
    void testRevokedTokenRejected() {
        // Try to use revoked token - should fail
        ApiClient revokedClient = new ApiClient();
        revokedClient.setBasePath("http://localhost:" + unityCatalogServer.getMappedPort(8080));
        revokedClient.addDefaultHeader("Authorization", "Bearer " + createdTokenValue);
        
        TokensApi revokedTokensApi = new TokensApi(revokedClient);
        
        ApiException exception = assertThrows(ApiException.class, 
            () -> revokedTokensApi.listTokens());
        
        assertEquals(401, exception.getCode());
        assertTrue(exception.getMessage().contains("revoked") || 
                  exception.getMessage().contains("invalid"));
    }

    @Test
    @Order(7)
    void testTokenExpiration() throws ApiException {
        // Create token with very short TTL
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Short-lived token")
                .lifetimeSeconds(1L); // 1 second

        CreateTokenResponse response = ownerTokensApi.createToken(request);
        String shortTokenValue = response.getTokenValue();
        
        // Wait for expiration
        try {
            Thread.sleep(2000); // Wait 2 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        }
        
        // Try to use expired token
        ApiClient expiredClient = new ApiClient();
        expiredClient.setBasePath("http://localhost:" + unityCatalogServer.getMappedPort(8080));
        expiredClient.addDefaultHeader("Authorization", "Bearer " + shortTokenValue);
        
        TokensApi expiredTokensApi = new TokensApi(expiredClient);
        
        ApiException exception = assertThrows(ApiException.class, 
            () -> expiredTokensApi.listTokens());
        
        assertEquals(401, exception.getCode());
        assertTrue(exception.getMessage().contains("expired") || 
                  exception.getMessage().contains("invalid"));
    }

    @Test
    @Order(8)
    void testUserTokenSelfManagement() throws ApiException {
        // Regular users should be able to manage their own tokens
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("User's own token");

        CreateTokenResponse response = userTokensApi.createToken(request);
        
        assertNotNull(response);
        assertNotNull(response.getTokenId());
        
        // Should be able to list their own tokens
        ListTokensResponse listResponse = userTokensApi.listTokens();
        assertNotNull(listResponse);
        assertTrue(listResponse.getTokens().stream()
                .anyMatch(token -> response.getTokenId().equals(token.getTokenId())));
        
        // Should be able to revoke their own token
        assertDoesNotThrow(() -> userTokensApi.revokeToken(response.getTokenId()));
    }

    @Test
    @Order(9)
    void testAdminTokenManagement() throws ApiException {
        // Admins should be able to see and manage all tokens
        // This test would verify admin capabilities vs regular user capabilities
        
        // Create token as regular user first
        CreateTokenRequest userRequest = new CreateTokenRequest()
                .comment("User token to be managed by admin");
        CreateTokenResponse userToken = userTokensApi.createToken(userRequest);
        
        // Admin should be able to see this token
        ListTokensResponse adminList = ownerTokensApi.listTokens();
        assertTrue(adminList.getTokens().stream()
                .anyMatch(token -> userToken.getTokenId().equals(token.getTokenId())));
        
        // Admin should be able to revoke user's token
        assertDoesNotThrow(() -> ownerTokensApi.revokeToken(userToken.getTokenId()));
    }

    @Test
    @Order(10)
    void testTokenSecurityProperties() throws ApiException {
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Security test token");

        CreateTokenResponse response = ownerTokensApi.createToken(request);
        
        // Token value should be cryptographically secure
        String tokenValue = response.getTokenValue();
        assertTrue(tokenValue.length() >= 32); // Minimum length
        assertFalse(tokenValue.contains("admin")); // No obvious patterns
        assertFalse(tokenValue.contains("user"));
        
        // Token should not be the same as token ID
        assertNotEquals(response.getTokenId(), tokenValue);
        
        // Multiple tokens should be different
        CreateTokenResponse response2 = ownerTokensApi.createToken(request);
        assertNotEquals(response.getTokenValue(), response2.getTokenValue());
    }
}
