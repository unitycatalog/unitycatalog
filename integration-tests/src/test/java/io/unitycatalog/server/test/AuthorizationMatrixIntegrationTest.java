package io.unitycatalog.server.test;

import io.unitycatalog.server.sdk.ApiClient;
import io.unitycatalog.server.sdk.ApiException;
import io.unitycatalog.server.sdk.api.*;
import io.unitycatalog.server.sdk.model.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Authorization Matrix Integration Tests.
 * Tests the authorization model across different privilege levels and endpoint categories.
 * Validates that BOOTSTRAP_OWNER, OWNER, admin, writer, and reader roles have appropriate access.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AuthorizationMatrixIntegrationTest {

    @Container
    static GenericContainer<?> unityCatalogServer = new GenericContainer<>("unitycatalog:test")
            .withExposedPorts(8080)
            .withEnv("UC_BOOTSTRAP_ENABLED", "true");

    // API clients for different privilege levels
    private static AdminBootstrapApi bootstrapOwnerBootstrapApi;
    private static CatalogsApi bootstrapOwnerCatalogsApi;
    private static GrantsApi bootstrapOwnerGrantsApi;
    private static TokensApi bootstrapOwnerTokensApi;
    
    private static CatalogsApi ownerCatalogsApi;
    private static GrantsApi ownerGrantsApi;
    private static TokensApi ownerTokensApi;
    
    private static CatalogsApi adminCatalogsApi;
    private static GrantsApi adminGrantsApi;
    private static TokensApi adminTokensApi;
    
    private static CatalogsApi writerCatalogsApi;
    private static GrantsApi writerGrantsApi;
    private static TokensApi writerTokensApi;
    
    private static CatalogsApi readerCatalogsApi;
    private static GrantsApi readerGrantsApi;
    private static TokensApi readerTokensApi;

    private static String testCatalogName = "test_catalog";

    @BeforeAll
    static void setUp() {
        String baseUrl = "http://localhost:" + unityCatalogServer.getMappedPort(8080);
        
        // Bootstrap Owner (highest privilege)
        ApiClient bootstrapOwnerClient = createClient(baseUrl, "bootstrap-owner-token");
        bootstrapOwnerBootstrapApi = new AdminBootstrapApi(bootstrapOwnerClient);
        bootstrapOwnerCatalogsApi = new CatalogsApi(bootstrapOwnerClient);
        bootstrapOwnerGrantsApi = new GrantsApi(bootstrapOwnerClient);
        bootstrapOwnerTokensApi = new TokensApi(bootstrapOwnerClient);
        
        // Regular Owner
        ApiClient ownerClient = createClient(baseUrl, "owner-token");
        ownerCatalogsApi = new CatalogsApi(ownerClient);
        ownerGrantsApi = new GrantsApi(ownerClient);
        ownerTokensApi = new TokensApi(ownerClient);
        
        // Admin
        ApiClient adminClient = createClient(baseUrl, "admin-token");
        adminCatalogsApi = new CatalogsApi(adminClient);
        adminGrantsApi = new GrantsApi(adminClient);
        adminTokensApi = new TokensApi(adminClient);
        
        // Writer
        ApiClient writerClient = createClient(baseUrl, "writer-token");
        writerCatalogsApi = new CatalogsApi(writerClient);
        writerGrantsApi = new GrantsApi(writerClient);
        writerTokensApi = new TokensApi(writerClient);
        
        // Reader
        ApiClient readerClient = createClient(baseUrl, "reader-token");
        readerCatalogsApi = new CatalogsApi(readerClient);
        readerGrantsApi = new GrantsApi(readerClient);
        readerTokensApi = new TokensApi(readerClient);
    }

    // Bootstrap Endpoint Tests - Only BOOTSTRAP_OWNER should have access
    
    @Test
    @Order(1)
    void testBootstrapEndpointBootstrapOwnerAccess() {
        // BOOTSTRAP_OWNER should have access
        assertDoesNotThrow(() -> {
            BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                    .azureJwt("mock.jwt.admin")
                    .upn("admin@contoso.com");
            bootstrapOwnerBootstrapApi.bootstrapOwner(request);
        });
    }

    @Test
    @Order(2)
    void testBootstrapEndpointOwnerDenied() {
        // Regular OWNER should NOT have access to bootstrap endpoint
        ApiClient ownerClient = createClient("http://localhost:" + unityCatalogServer.getMappedPort(8080), "owner-token");
        AdminBootstrapApi ownerBootstrapApi = new AdminBootstrapApi(ownerClient);
        
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt("mock.jwt.admin")
                .upn("admin@contoso.com");
        
        ApiException exception = assertThrows(ApiException.class, 
            () -> ownerBootstrapApi.bootstrapOwner(request));
        assertEquals(403, exception.getCode());
    }

    @Test
    @Order(3)
    void testBootstrapEndpointLowerPrivilegesDenied() {
        // Admin, Writer, Reader should NOT have access
        List<AdminBootstrapApi> deniedClients = Arrays.asList(
            new AdminBootstrapApi(createClient("http://localhost:" + unityCatalogServer.getMappedPort(8080), "admin-token")),
            new AdminBootstrapApi(createClient("http://localhost:" + unityCatalogServer.getMappedPort(8080), "writer-token")),
            new AdminBootstrapApi(createClient("http://localhost:" + unityCatalogServer.getMappedPort(8080), "reader-token"))
        );
        
        BootstrapOwnerRequest request = new BootstrapOwnerRequest()
                .azureJwt("mock.jwt.admin")
                .upn("admin@contoso.com");
        
        for (AdminBootstrapApi api : deniedClients) {
            ApiException exception = assertThrows(ApiException.class, 
                () -> api.bootstrapOwner(request));
            assertEquals(403, exception.getCode());
        }
    }

    // Catalog Management Tests
    
    @Test
    @Order(4)
    void testCatalogCreatePermissions() throws ApiException {
        CreateCatalogRequest request = new CreateCatalogRequest()
                .name(testCatalogName)
                .comment("Test catalog for authorization matrix");
        
        // BOOTSTRAP_OWNER and OWNER should be able to create catalogs
        assertDoesNotThrow(() -> bootstrapOwnerCatalogsApi.createCatalog(request));
        
        // Clean up and test OWNER
        bootstrapOwnerCatalogsApi.deleteCatalog(testCatalogName, true);
        assertDoesNotThrow(() -> ownerCatalogsApi.createCatalog(request));
        
        // Admin should be able to create catalogs
        ownerCatalogsApi.deleteCatalog(testCatalogName, true);
        assertDoesNotThrow(() -> adminCatalogsApi.createCatalog(request));
        
        // Writer should NOT be able to create catalogs
        adminCatalogsApi.deleteCatalog(testCatalogName, true);
        ApiException writerException = assertThrows(ApiException.class, 
            () -> writerCatalogsApi.createCatalog(request));
        assertEquals(403, writerException.getCode());
        
        // Reader should NOT be able to create catalogs
        ApiException readerException = assertThrows(ApiException.class, 
            () -> readerCatalogsApi.createCatalog(request));
        assertEquals(403, readerException.getCode());
        
        // Create catalog for subsequent tests
        bootstrapOwnerCatalogsApi.createCatalog(request);
    }

    @Test
    @Order(5)
    void testCatalogReadPermissions() throws ApiException {
        // All privilege levels should be able to read catalogs
        assertDoesNotThrow(() -> bootstrapOwnerCatalogsApi.getCatalog(testCatalogName));
        assertDoesNotThrow(() -> ownerCatalogsApi.getCatalog(testCatalogName));
        assertDoesNotThrow(() -> adminCatalogsApi.getCatalog(testCatalogName));
        assertDoesNotThrow(() -> writerCatalogsApi.getCatalog(testCatalogName));
        assertDoesNotThrow(() -> readerCatalogsApi.getCatalog(testCatalogName));
        
        // All should be able to list catalogs
        assertDoesNotThrow(() -> bootstrapOwnerCatalogsApi.listCatalogs(null, null));
        assertDoesNotThrow(() -> ownerCatalogsApi.listCatalogs(null, null));
        assertDoesNotThrow(() -> adminCatalogsApi.listCatalogs(null, null));
        assertDoesNotThrow(() -> writerCatalogsApi.listCatalogs(null, null));
        assertDoesNotThrow(() -> readerCatalogsApi.listCatalogs(null, null));
    }

    @Test
    @Order(6)
    void testCatalogUpdatePermissions() throws ApiException {
        UpdateCatalogRequest request = new UpdateCatalogRequest()
                .comment("Updated comment");
        
        // BOOTSTRAP_OWNER, OWNER, and Admin should be able to update catalogs
        assertDoesNotThrow(() -> bootstrapOwnerCatalogsApi.updateCatalog(testCatalogName, request));
        assertDoesNotThrow(() -> ownerCatalogsApi.updateCatalog(testCatalogName, request));
        assertDoesNotThrow(() -> adminCatalogsApi.updateCatalog(testCatalogName, request));
        
        // Writer should NOT be able to update catalogs
        ApiException writerException = assertThrows(ApiException.class, 
            () -> writerCatalogsApi.updateCatalog(testCatalogName, request));
        assertEquals(403, writerException.getCode());
        
        // Reader should NOT be able to update catalogs
        ApiException readerException = assertThrows(ApiException.class, 
            () -> readerCatalogsApi.updateCatalog(testCatalogName, request));
        assertEquals(403, readerException.getCode());
    }

    // Grants Management Tests
    
    @Test
    @Order(7)
    void testGrantsListPermissions() throws ApiException {
        // BOOTSTRAP_OWNER, OWNER, and Admin should be able to list grants
        assertDoesNotThrow(() -> bootstrapOwnerGrantsApi.listGrants(null, null, null));
        assertDoesNotThrow(() -> ownerGrantsApi.listGrants(null, null, null));
        assertDoesNotThrow(() -> adminGrantsApi.listGrants(null, null, null));
        
        // Writer should NOT be able to list grants
        ApiException writerException = assertThrows(ApiException.class, 
            () -> writerGrantsApi.listGrants(null, null, null));
        assertEquals(403, writerException.getCode());
        
        // Reader should NOT be able to list grants
        ApiException readerException = assertThrows(ApiException.class, 
            () -> readerGrantsApi.listGrants(null, null, null));
        assertEquals(403, readerException.getCode());
    }

    @Test
    @Order(8)
    void testGrantsUpdatePermissions() {
        GrantRequest request = new GrantRequest()
                .principal(new Principal().name("test@contoso.com").type(PrincipalType.USER))
                .privilege(Privilege.USE_CATALOG);
        
        // BOOTSTRAP_OWNER, OWNER, and Admin should be able to grant permissions
        assertDoesNotThrow(() -> bootstrapOwnerGrantsApi.updateGrant(request));
        assertDoesNotThrow(() -> ownerGrantsApi.updateGrant(request));
        assertDoesNotThrow(() -> adminGrantsApi.updateGrant(request));
        
        // Writer should NOT be able to grant permissions
        ApiException writerException = assertThrows(ApiException.class, 
            () -> writerGrantsApi.updateGrant(request));
        assertEquals(403, writerException.getCode());
        
        // Reader should NOT be able to grant permissions
        ApiException readerException = assertThrows(ApiException.class, 
            () -> readerGrantsApi.updateGrant(request));
        assertEquals(403, readerException.getCode());
    }

    // Token Management Tests
    
    @Test
    @Order(9)
    void testTokenSelfManagement() throws ApiException {
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Self-managed token");
        
        // All privilege levels should be able to create their own tokens
        assertDoesNotThrow(() -> bootstrapOwnerTokensApi.createToken(request));
        assertDoesNotThrow(() -> ownerTokensApi.createToken(request));
        assertDoesNotThrow(() -> adminTokensApi.createToken(request));
        assertDoesNotThrow(() -> writerTokensApi.createToken(request));
        assertDoesNotThrow(() -> readerTokensApi.createToken(request));
        
        // All should be able to list their own tokens
        assertDoesNotThrow(() -> bootstrapOwnerTokensApi.listTokens());
        assertDoesNotThrow(() -> ownerTokensApi.listTokens());
        assertDoesNotThrow(() -> adminTokensApi.listTokens());
        assertDoesNotThrow(() -> writerTokensApi.listTokens());
        assertDoesNotThrow(() -> readerTokensApi.listTokens());
    }

    @Test
    @Order(10)
    void testTokenAdminManagement() throws ApiException {
        // Create token as a writer
        CreateTokenRequest request = new CreateTokenRequest()
                .comment("Token for admin management test");
        CreateTokenResponse writerToken = writerTokensApi.createToken(request);
        
        // BOOTSTRAP_OWNER, OWNER, and Admin should be able to see all tokens
        // and revoke other users' tokens
        assertDoesNotThrow(() -> bootstrapOwnerTokensApi.revokeToken(writerToken.getTokenId()));
        
        // Create another token for owner test
        CreateTokenResponse writerToken2 = writerTokensApi.createToken(request);
        assertDoesNotThrow(() -> ownerTokensApi.revokeToken(writerToken2.getTokenId()));
        
        // Create another token for admin test
        CreateTokenResponse writerToken3 = writerTokensApi.createToken(request);
        assertDoesNotThrow(() -> adminTokensApi.revokeToken(writerToken3.getTokenId()));
        
        // Writer should NOT be able to revoke other users' tokens
        CreateTokenResponse readerToken = readerTokensApi.createToken(request);
        ApiException writerException = assertThrows(ApiException.class, 
            () -> writerTokensApi.revokeToken(readerToken.getTokenId()));
        assertEquals(403, writerException.getCode());
        
        // Reader should NOT be able to revoke other users' tokens
        CreateTokenResponse adminToken = adminTokensApi.createToken(request);
        ApiException readerException = assertThrows(ApiException.class, 
            () -> readerTokensApi.revokeToken(adminToken.getTokenId()));
        assertEquals(403, readerException.getCode());
    }

    private static ApiClient createClient(String baseUrl, String token) {
        ApiClient client = new ApiClient();
        client.setBasePath(baseUrl);
        client.addDefaultHeader("Authorization", "Bearer " + token);
        return client;
    }
}
