package io.unitycatalog.server.utils;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.Map;

/**
 * Manual testing utility to generate Azure AD-compliant JWTs for testing the bootstrap endpoint.
 *
 * <p>This is intended for manual testing and integration testing scenarios where you need a valid
 * Azure AD JWT structure but don't have access to a real Azure AD tenant.
 *
 * <p>WARNING: This is for testing only! In production, you need real Azure AD tokens. For unit
 * tests, use mocks like in AdminBootstrapServiceTest.java.
 */
public class AzureJwtTestGenerator {

  private final Algorithm algorithm;
  private final String tenantId;

  public AzureJwtTestGenerator(String tenantId) throws Exception {
    this.tenantId = tenantId;

    // Generate a test RSA key pair (in production, Azure AD uses real keys)
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keyPair = generator.generateKeyPair();

    RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
    RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

    this.algorithm = Algorithm.RSA256(publicKey, privateKey);
  }

  public String createTestAzureJwt(
      String userPrincipalName, String displayName, String azureObjectId) {
    return JWT.create()
        .withIssuer("https://login.microsoftonline.com/" + tenantId + "/v2.0")
        .withSubject(azureObjectId)
        .withAudience("your-application-id") // This would be your Azure app registration ID
        .withIssuedAt(new Date())
        .withExpiresAt(new Date(System.currentTimeMillis() + 3600000)) // 1 hour
        .withKeyId("test-key-id")
        .withClaim("oid", azureObjectId) // Azure Object ID
        .withClaim("preferred_username", userPrincipalName) // UPN
        .withClaim("name", displayName) // Display name
        .withClaim("tid", tenantId) // Tenant ID
        .withClaim("ver", "2.0") // Version
        .sign(algorithm);
  }

  public static void main(String[] args) throws Exception {
    String tenantId = "your-tenant-id"; // Your tenant ID from server.properties
    String clientId = "your-client-id"; // Your client ID from server.properties

    // Replace with actual values during testing
    Map<String, Object> claims =
        Map.of(
            "aud",
            clientId,
            "iss",
            "https://sts.windows.net/" + tenantId + "/",
            "iat",
            System.currentTimeMillis() / 1000,
            "exp",
            (System.currentTimeMillis() / 1000) + 3600,
            "sub",
            "your-subject-id",
            "upn",
            "your-username@yourdomain.com", // Your UPN
            "email",
            "your-email@yourdomain.com" // Your email
            );

    TestAzureJwtGenerator generator = new AzureJwtTestGenerator(tenantId);

    String testJwt =
        generator.createTestAzureJwt(
            "Ray_Harrison@cable.comcast.com", // Your UPN
            "Ray Harrison", // Display name
            "test-azure-object-id-12345" // Mock Azure Object ID
            );

    System.out.println("Test Azure AD JWT:");
    System.out.println(testJwt);
    System.out.println();
    System.out.println("Test command:");
    System.out.println(
        "curl -i -X POST http://localhost:8080/api/2.1/unity-catalog/admins/bootstrap-owner \\");
    System.out.println("  -H \"Content-Type: application/json\" \\");
    System.out.println("  -H \"Authorization: Bearer " + testJwt + "\" \\");
    System.out.println("  -d '{\"metastoreId\": \"test-metastore-123\"}'");
  }
}
