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
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Files;

public class GcpCredentialVendor {

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");

  // Google's limit on a credential access boundary condition.
  static final int MAX_CONDITION_LENGTH = 2048;

  private final Map<NormalizedURL, GcsStorageConfig> gcsConfigurations;
  private final Map<NormalizedURL, GcpCredentialGenerator> credentialGenerators =
      new ConcurrentHashMap<>();

  public GcpCredentialVendor(ServerProperties serverProperties) {
    this.gcsConfigurations = serverProperties.getGcsConfigurations();
  }

  public AccessToken vendGcpCredential(CredentialContext credentialContext) {
    credentialContext
        .getCredentialDAO()
        .ifPresent(
            c -> {
              throw new BaseException(
                  ErrorCode.UNIMPLEMENTED,
                  "Storage credential/external location for GCP is not supported yet.");
            });

    NormalizedURL storageBase = credentialContext.getStorageBase();
    GcsStorageConfig storageConfig = gcsConfigurations.get(storageBase);

    if (storageConfig == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          format("Unknown GCS storage configuration for %s.", storageBase));
    }

    GcpCredentialGenerator generator =
        credentialGenerators.computeIfAbsent(storageBase, key -> createGenerator(storageConfig));
    return generator.generate(credentialContext);
  }

  private GcpCredentialGenerator createGenerator(GcsStorageConfig storageConfig) {
    String generatorClass = storageConfig.getCredentialGenerator();
    if (generatorClass != null && !generatorClass.isEmpty()) {
      try {
        Class<? extends GcpCredentialGenerator> generatorType =
            Class.forName(generatorClass).asSubclass(GcpCredentialGenerator.class);
        try {
          return generatorType
              .getDeclaredConstructor(GcsStorageConfig.class)
              .newInstance(storageConfig);
        } catch (NoSuchMethodException e) {
          return generatorType.getDeclaredConstructor().newInstance();
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to instantiate GCS credentials generator " + generatorClass, e);
      }
    }

    String jsonKeyFilePath = storageConfig.getJsonKeyFilePath();
    if (jsonKeyFilePath != null && jsonKeyFilePath.isEmpty()) {
      jsonKeyFilePath = null;
    }

    return new ServiceAccountCredentialGenerator(jsonKeyFilePath);
  }

  /**
   * Converts text into a double-quoted CEL string literal.
   *
   * <p>GCS object names may contain characters, such as quotation marks and backslashes, that CEL
   * uses as part of its syntax. Escaping each character while building the literal keeps the object
   * name inside the string instead of letting it change the credential access boundary condition.
   *
   * <p>The literal uses double quotes because this method escapes double quotes. Changing the
   * delimiter to single quotes without also changing the escape rules would make apostrophes
   * unsafe.
   *
   * @see <a
   *     href="https://github.com/cel-expr/cel-spec/blob/master/doc/langdef.md#string-and-bytes-values">
   *     CEL string and bytes values</a>
   * @see <a
   *     href="https://github.com/cel-expr/cel-java/blob/d4c891326ebcccb632a813c4a3b3f08ca032889b/extensions/src/main/java/dev/cel/extensions/CelStringExtensions.java#L471-L512">
   *     CEL Java string quoting implementation</a>
   */
  private static String celStringLiteral(String value) {
    StringBuilder literal = new StringBuilder(value.length() + 2).append('"');
    for (int offset = 0; offset < value.length(); ) {
      int codePoint = value.codePointAt(offset);
      offset += Character.charCount(codePoint);
      switch (codePoint) {
        case '\u0007' -> literal.append("\\a");
        case '\b' -> literal.append("\\b");
        case '\f' -> literal.append("\\f");
        case '\n' -> literal.append("\\n");
        case '\r' -> literal.append("\\r");
        case '\t' -> literal.append("\\t");
        case '\u000B' -> literal.append("\\v");
        case '\\' -> literal.append("\\\\");
        case '"' -> literal.append("\\\"");
        default -> literal.appendCodePoint(codePoint);
      }
    }
    return literal.append('"').toString();
  }

  OAuth2Credentials downscopeGcpCreds(GoogleCredentials credentials, CredentialContext context) {
    CredentialAccessBoundary.Builder boundaryBuilder = CredentialAccessBoundary.newBuilder();
    List<String> roles = resolvePrivilegesToRoles(context.getPrivileges());

    context
        .getLocations()
        .forEach(
            location -> {
              URI locationUri = location.toUri();
              String path = CharMatcher.is('/').trimLeadingFrom(locationUri.getPath());

              String resource =
                  format("//storage.googleapis.com/projects/_/buckets/%s", locationUri.getHost());

              // Match the path itself or anything under path/. Listing with the bare path as the
              // prefix also returns sibling names such as path_archive, as the S3 policy allows.
              String dirPrefix = path.isEmpty() ? "" : path + "/";
              String objectsPrefix =
                  format("projects/_/buckets/%s/objects/", locationUri.getHost());

              // for reading/writing objects
              String objectStartsWithExpr =
                  format(
                      "resource.name.startsWith(%s)", celStringLiteral(objectsPrefix + dirPrefix));

              // for listing objects
              String listStartsWithExpr =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix',"
                          + " '').startsWith(%s)",
                      celStringLiteral(dirPrefix));

              // At a bucket root the startsWith clauses already match everything.
              String combinedExpr =
                  path.isEmpty()
                      ? objectStartsWithExpr + " || " + listStartsWithExpr
                      : format(
                          "resource.name == %s || %s || api.getAttribute("
                              + "'storage.googleapis.com/objectListPrefix', '') == %s || %s",
                          celStringLiteral(objectsPrefix + path),
                          objectStartsWithExpr,
                          celStringLiteral(path),
                          listStartsWithExpr);

              if (combinedExpr.length() > MAX_CONDITION_LENGTH) {
                throw new BaseException(
                    ErrorCode.INVALID_ARGUMENT,
                    format(
                        "GCS location %s is too long for a downscoped credential: its access"
                            + " boundary condition is %d characters, and the limit is %d.",
                        location, combinedExpr.length(), MAX_CONDITION_LENGTH));
              }

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

  /**
   * Maps a failure to get a downscoped token. When the token service rejects the request as
   * invalid, for example because the access boundary is over its size limit, the location is the
   * cause, so the error is {@code INVALID_ARGUMENT}.
   */
  static BaseException credentialError(IOException e) {
    // The token service's OAuth error, formatted by google-auth as "Error code <code>: <text>".
    if (e.getMessage() != null && e.getMessage().startsWith("Error code invalid_request")) {
      return new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "GCS rejected the downscoped credential request: " + e.getMessage(),
          e);
    }
    return new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
  }

  List<String> resolvePrivilegesToRoles(Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }

  private final class ServiceAccountCredentialGenerator implements GcpCredentialGenerator {
    private final String jsonKeyFilePath;

    private ServiceAccountCredentialGenerator(String jsonKeyFilePath) {
      this.jsonKeyFilePath = jsonKeyFilePath;
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      try {
        GoogleCredentials creds;
        if (jsonKeyFilePath != null && !jsonKeyFilePath.isEmpty()) {
          creds =
              ServiceAccountCredentials.fromStream(Files.localInput(jsonKeyFilePath).newStream());
        } else {
          creds = GoogleCredentials.getApplicationDefault();
        }
        return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), context).refreshAccessToken();
      } catch (IOException e) {
        throw credentialError(e);
      }
    }
  }
}
