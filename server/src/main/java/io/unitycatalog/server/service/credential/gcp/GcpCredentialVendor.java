package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.net.URI;
import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.iceberg.Files;

public class GcpCredentialVendor {

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");

  private final Map<String, String> gcsConfigurations;

  public GcpCredentialVendor() {
    this.gcsConfigurations = ServerPropertiesUtils.getInstance().getGcsConfigurations();
  }

  @SneakyThrows
  public AccessToken vendGcpToken(CredentialContext credentialContext) {
    String serviceAccountKeyJsonFilePath =
        gcsConfigurations.get(credentialContext.getStorageBase());

    GoogleCredentials creds;
    if (serviceAccountKeyJsonFilePath != null && !serviceAccountKeyJsonFilePath.isEmpty()) {
      if (serviceAccountKeyJsonFilePath.startsWith("testing://")) {
        // allow pass-through of a dummy value for integration testing
        return AccessToken.newBuilder()
            .setTokenValue(serviceAccountKeyJsonFilePath)
            .setExpirationTime(Date.valueOf("9999-01-01"))
            .build();
      }
      creds =
          ServiceAccountCredentials.fromStream(
              Files.localInput(serviceAccountKeyJsonFilePath).newStream());
    } else {
      creds = GoogleCredentials.getApplicationDefault();
    }

    return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), credentialContext)
        .refreshAccessToken();
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
              String expr =
                  format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      locationUri.getHost(), path);

              boundaryBuilder.addRule(
                  CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                      .setAvailablePermissions(roles)
                      .setAvailabilityCondition(
                          CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
                              .newBuilder()
                              .setExpression(expr)
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
}
