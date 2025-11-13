package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Files;

/**
 * Vends GCS OAuth tokens for Unity Catalog by downscoping credentials against the configured bucket
 * allow list.
 *
 * <p>Administrators can either rely on default service-account credentials or plug in a custom
 * {@link GcpCredentialsGenerator} using the {@code gcs.credentialsGenerator.*} property.
 */
public class GcpCredentialVendor {

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");

  private final Map<String, GcsStorageConfig> gcsConfigurations;
  private final Map<String, GcpCredentialsGenerator> credentialGenerators =
      new ConcurrentHashMap<>();

  /**
   * Pluggable contract for generating GCS OAuth tokens. This is used to allow for custom credential
   * generation for local integration tests.
   */
  public interface GcpCredentialsGenerator {
    AccessToken generate(CredentialContext context);
  }

  public GcpCredentialVendor(ServerProperties serverProperties) {
    this.gcsConfigurations = serverProperties.getGcsConfigurations();
  }

  public AccessToken vendGcpToken(CredentialContext credentialContext) {
    String storageBase = credentialContext.getStorageBase();
    GcsStorageConfig storageConfig = gcsConfigurations.get(storageBase);

    // Cache a generator per bucket so repeated requests reuse the same credential strategy.
    GcpCredentialsGenerator generator =
        credentialGenerators.computeIfAbsent(
            storageBase, key -> createGenerator(key, storageConfig));
    return generator.generate(credentialContext);
  }

  // Resolve user-provided generators first, otherwise fall back to service-account credentials.
  private GcpCredentialsGenerator createGenerator(
      String storageBase, GcsStorageConfig storageConfig) {
    if (storageConfig != null) {
      String generatorClass = storageConfig.getCredentialsGenerator();
      if (generatorClass != null && !generatorClass.isEmpty()) {
        try {
          return (GcpCredentialsGenerator)
              Class.forName(generatorClass).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new RuntimeException(
              "Unable to instantiate GCS credentials generator " + generatorClass, e);
        }
      }
    }

    String jsonKeyFilePath = storageConfig != null ? storageConfig.getJsonKeyFilePath() : null;
    if (jsonKeyFilePath != null && jsonKeyFilePath.isEmpty()) {
      jsonKeyFilePath = null;
    }

    // Default behavior: produce service-account derived tokens for real deployments.
    return new ServiceAccountCredentialsGenerator(jsonKeyFilePath);
  }

  OAuth2Credentials downscopeGcpCreds(GoogleCredentials credentials, CredentialContext context) {
    CredentialAccessBoundary.Builder boundaryBuilder = CredentialAccessBoundary.newBuilder();
    List<String> roles = resolvePrivilegesToRoles(context.getPrivileges());

    context
        .getLocations()
        .forEach(
            location -> {
              URI locationUri = URI.create(location);
              String path = CharMatcher.is('/').trimLeadingFrom(locationUri.getPath());

              String resource =
                  format("//storage.googleapis.com/projects/_/buckets/%s", locationUri.getHost());

              // for reading/writing objects
              String resourceNameStartsWithExpr =
                  format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      locationUri.getHost(), path);

              // for listing objects
              String objectListPrefixStartsWithExpr =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      path);

              String combinedExpr =
                  resourceNameStartsWithExpr + " || " + objectListPrefixStartsWithExpr;

              boundaryBuilder.addRule(
                  CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                      .setAvailablePermissions(roles)
                      .setAvailabilityCondition(
                          CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
                              .newBuilder()
                              .setExpression(combinedExpr)
                              .build())
                      .setAvailableResource(resource)
                      .build());
            });

    return DownscopedCredentials.newBuilder()
        .setSourceCredential(credentials)
        .setCredentialAccessBoundary(boundaryBuilder.build())
        .build();
  }

  List<String> resolvePrivilegesToRoles(Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }

  /**
   * Generates access tokens by loading service-account JSON when specified, or by falling back to
   * Google application-default credentials. Tokens are then downscoped to the caller’s bucket
   * privileges, matching the production flow used outside of tests.
   */
  private final class ServiceAccountCredentialsGenerator implements GcpCredentialsGenerator {
    private final String jsonKeyFilePath;

    private ServiceAccountCredentialsGenerator(String jsonKeyFilePath) {
      // Retain the optional service-account path so we can load explicit keys when present.
      this.jsonKeyFilePath = jsonKeyFilePath;
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      try {
        GoogleCredentials creds;
        if (jsonKeyFilePath != null && !jsonKeyFilePath.isEmpty()) {
          // Local files leverage the Iceberg helper to load service-account JSON.
          creds =
              ServiceAccountCredentials.fromStream(Files.localInput(jsonKeyFilePath).newStream());
        } else {
          // Otherwise fall back to GCP default credentials configured on the host.
          creds = GoogleCredentials.getApplicationDefault();
        }
        return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), context).refreshAccessToken();
      } catch (IOException e) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
      }
    }
  }
}
