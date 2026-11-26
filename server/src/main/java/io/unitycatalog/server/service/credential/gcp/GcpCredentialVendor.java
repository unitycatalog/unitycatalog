package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.api.client.http.HttpResponseException;
import com.google.auth.Retryable;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.model.GcpServiceAccountKeyResponse;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.secret.CredentialSecretStore;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.GcpApplicationDefaultCredentials;
import io.unitycatalog.server.utils.GcpServiceAccountKeys;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.iceberg.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpCredentialVendor {
  private static final Logger LOGGER = LoggerFactory.getLogger(GcpCredentialVendor.class);

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");

  // Google's limit on a credential access boundary condition.
  static final int MAX_CONDITION_LENGTH = 2048;

  private final Map<NormalizedURL, GcsStorageConfig> gcsConfigurations;
  private final Map<NormalizedURL, GcpCredentialGenerator> credentialGenerators =
      new ConcurrentHashMap<>();
  private final CredentialSecretStore secretStore;
  private final Function<GcpServiceAccountKeyRequest, GcpCredentialGenerator> keyGenerators;
  private final GcpApplicationDefaultCredentials defaultCredentials;
  private final GcpCredentialGenerator serverIdentityGenerator;
  // How Google's token endpoints are reached, or null for the library's default; tests pass a mock.
  private final HttpTransportFactory transportFactory;

  public GcpCredentialVendor(ServerProperties serverProperties) {
    this(serverProperties, GcpApplicationDefaultCredentials.DEFAULT);
  }

  public GcpCredentialVendor(
      ServerProperties serverProperties, GcpApplicationDefaultCredentials defaultCredentials) {
    this(
        serverProperties,
        CredentialSecretStore.from(serverProperties),
        null,
        defaultCredentials,
        null);
  }

  /**
   * @param secretStore decrypts the private key of a stored service account key
   * @param keyGenerators makes the generator of a stored service account key, or null to sign in
   *     with the key; tests pass one that does not call Google
   */
  GcpCredentialVendor(
      ServerProperties serverProperties,
      CredentialSecretStore secretStore,
      Function<GcpServiceAccountKeyRequest, GcpCredentialGenerator> keyGenerators) {
    this(
        serverProperties,
        secretStore,
        keyGenerators,
        GcpApplicationDefaultCredentials.DEFAULT,
        null);
  }

  /**
   * @param transportFactory how the stored key signs in and how its token is downscoped, or null
   *     for the library's default; tests pass one that does not call Google
   */
  GcpCredentialVendor(
      ServerProperties serverProperties,
      CredentialSecretStore secretStore,
      Function<GcpServiceAccountKeyRequest, GcpCredentialGenerator> keyGenerators,
      HttpTransportFactory transportFactory) {
    this(
        serverProperties,
        secretStore,
        keyGenerators,
        GcpApplicationDefaultCredentials.DEFAULT,
        transportFactory);
  }

  /**
   * @param defaultCredentials loads the server's Application Default Credentials for {@code
   *     gcp_server_identity} credentials
   * @param transportFactory how the stored key signs in and how its token is downscoped, or null
   *     for the library's default; tests pass one that does not call Google
   */
  GcpCredentialVendor(
      ServerProperties serverProperties,
      CredentialSecretStore secretStore,
      Function<GcpServiceAccountKeyRequest, GcpCredentialGenerator> keyGenerators,
      GcpApplicationDefaultCredentials defaultCredentials,
      HttpTransportFactory transportFactory) {
    this.gcsConfigurations = serverProperties.getGcsConfigurations();
    this.secretStore = secretStore;
    this.keyGenerators = keyGenerators != null ? keyGenerators : this::createKeyGenerator;
    this.defaultCredentials =
        Objects.requireNonNullElse(defaultCredentials, GcpApplicationDefaultCredentials.DEFAULT);
    this.transportFactory = transportFactory;
    this.serverIdentityGenerator = createServerIdentityGenerator();
  }

  public AccessToken vendGcpCredential(CredentialContext credentialContext) {
    Optional<CredentialDAO> credentialDAO = credentialContext.getCredentialDAO();
    if (credentialDAO.isPresent()) {
      CredentialDAO dao = credentialDAO.get();
      GcpCredentialGenerator generator =
          dao.getCredentialType() == CredentialDAO.CredentialType.GCP_SERVER_IDENTITY
              ? serverIdentityGenerator
              // The storage credential of the external location holds the service account key (or
              // is a non-GCP credential type that storedKey rejects without naming the credential).
              : keyGenerators.apply(storedKey(dao));
      try {
        return generator.generate(credentialContext);
      } catch (BaseException e) {
        // The caller gets a message without the credential; the administrator gets it here.
        LOGGER.warn(
            "Cannot get a token with storage credential '{}': {}",
            dao.getName(),
            e.getMessage(),
            e);
        throw e;
      }
    }

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

  /**
   * Reads the service account key of a storage credential. Whoever asks for access may not be
   * allowed to read the credential, so a failure names none of it publicly; the cause is logged.
   */
  private GcpServiceAccountKeyRequest storedKey(CredentialDAO dao) {
    try {
      GcpServiceAccountKeyResponse key = dao.getGcpServiceAccountKeyResponse();
      return new GcpServiceAccountKeyRequest()
          .email(key.getEmail())
          .privateKeyId(key.getPrivateKeyId())
          .privateKey(dao.getGcpPrivateKey(secretStore));
    } catch (BaseException e) {
      LOGGER.warn(
          "Cannot read the key of storage credential '{}': {}", dao.getName(), e.getMessage(), e);
      throw new BaseException(
          e.getErrorCode(),
          "The storage credential for this location cannot be used. Contact the server"
              + " administrator.",
          e);
    }
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

    String path = jsonKeyFilePath;
    return new ServiceAccountCredentialGenerator(
        () ->
            path != null
                ? ServiceAccountCredentials.fromStream(Files.localInput(path).newStream())
                : GoogleCredentials.getApplicationDefault(),
        GcpCredentialVendor::credentialError);
  }

  private GcpCredentialGenerator createServerIdentityGenerator() {
    return new ServiceAccountCredentialGenerator(
        () -> {
          try {
            return defaultCredentials.load();
          } catch (IOException | RuntimeException e) {
            throw new BaseException(
                ErrorCode.FAILED_PRECONDITION,
                "The server has no usable Google Cloud Application Default Credentials. Contact the"
                    + " server administrator.",
                e);
          }
        },
        GcpCredentialVendor::serverIdentityError);
  }

  private GcpCredentialGenerator createKeyGenerator(GcpServiceAccountKeyRequest key) {
    return new ServiceAccountCredentialGenerator(
        () -> {
          try {
            ServiceAccountCredentials credentials = GcpServiceAccountKeys.credentialsOf(key);
            return transportFactory == null
                ? credentials
                : credentials.toBuilder().setHttpTransportFactory(transportFactory).build();
          } catch (IOException e) {
            // The key was checked when the credential was created, so this is the server's fault.
            throw new BaseException(
                ErrorCode.INTERNAL, "The stored service account key cannot be loaded.", e);
          }
        },
        GcpCredentialVendor::storedKeyError);
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

              // Not getHost(): it is null for a bucket name with an underscore.
              String bucket = locationUri.getRawAuthority();
              String resource = format("//storage.googleapis.com/projects/_/buckets/%s", bucket);

              // Read the path itself or anything under path/. A list must use a prefix under path/:
              // listing with the bare path as the prefix would return sibling names such as
              // path_archive.
              String dirPrefix = path.isEmpty() ? "" : path + "/";
              String objectsPrefix = format("projects/_/buckets/%s/objects/", bucket);

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
                          "resource.name == %s || %s || %s",
                          celStringLiteral(objectsPrefix + path),
                          objectStartsWithExpr,
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

    DownscopedCredentials.Builder downscoped =
        DownscopedCredentials.newBuilder()
            .setSourceCredential(credentials)
            .setCredentialAccessBoundary(boundaryBuilder.build());
    if (transportFactory != null) {
      downscoped.setHttpTransportFactory(transportFactory);
    }
    return downscoped.build();
  }

  /**
   * Maps a failure to get a downscoped token. When the token service rejects the request as
   * invalid, for example because the access boundary is over its size limit, the location is the
   * cause, so the error is {@code INVALID_ARGUMENT}.
   */
  static BaseException credentialError(Exception e) {
    // The token service's OAuth error, formatted by google-auth as "Error code <code>: <text>".
    if (e.getMessage() != null && e.getMessage().startsWith("Error code invalid_request")) {
      return new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "GCS rejected the downscoped credential request: " + e.getMessage(),
          e);
    }
    return new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
  }

  /**
   * Maps a failure to get a token with a stored service account key:
   *
   * <ul>
   *   <li>the token service rejecting the request as invalid, which is the location's doing, is
   *       {@code INVALID_ARGUMENT};
   *   <li>Google rejecting the key, for example because it was disabled or deleted, is for an
   *       administrator to fix: {@code FAILED_PRECONDITION}, saying to contact the administrator;
   *   <li>Google rejecting the server's timestamps, which is clock skew that no key fixes, is
   *       {@code INTERNAL};
   *   <li>any other answer from Google that the library marks as not worth retrying, such as a 403,
   *       is {@code FAILED_PRECONDITION} too;
   *   <li>any other failure, such as a network error, is {@code UNAVAILABLE}, since a retry may
   *       succeed.
   * </ul>
   *
   * No message names an account or key, since whoever asks for access may not be allowed to read
   * the credential.
   */
  static BaseException storedKeyError(Exception e) {
    return gcpTokenError(
        e,
        "Google rejected the service account key of the storage credential for this location:"
            + " it may have been disabled, deleted or revoked. Contact the server administrator.",
        /* checkMetadataErrors= */ false);
  }

  /**
   * Maps a failure to get a downscoped token using the server's Application Default Credentials.
   * Same error classification as {@link #storedKeyError}, plus Compute Engine metadata 401/403/404
   * (unauthorized/forbidden or no service account / scopes on the instance) as {@code
   * FAILED_PRECONDITION}.
   */
  static BaseException serverIdentityError(Exception e) {
    return gcpTokenError(
        e,
        "Google rejected the server's Google Cloud credentials for this location. Contact the"
            + " server administrator.",
        /* checkMetadataErrors= */ true);
  }

  private static BaseException gcpTokenError(
      Exception e, String rejectedCredentialsMessage, boolean checkMetadataErrors) {
    if (String.valueOf(e.getMessage()).startsWith("Error code invalid_request")) {
      return credentialError(e);
    }
    // Google's answer to the source credential is in the causes: the downscoping step reports only
    // that it could not refresh the source credential. The strings are the error codes and
    // descriptions of Google's OAuth token endpoint, as google-auth puts them in its exception
    // messages.
    String reasons = causeMessages(e);
    // invalid_grant also means the server's clock is off, which the key cannot fix.
    if (reasons.contains("Token must be a short-lived token")) {
      return new BaseException(
          ErrorCode.INTERNAL,
          "Google rejected the server's sign-in timestamps. Contact the server administrator.",
          e);
    }
    if (reasons.contains("invalid_grant")
        || reasons.contains("invalid_client")
        || reasons.contains("unauthorized_client")) {
      return new BaseException(ErrorCode.FAILED_PRECONDITION, rejectedCredentialsMessage, e);
    }
    if (checkMetadataErrors
        && (reasons.contains("Error code 401 trying to get security access token")
            || reasons.contains("Error code 403 trying to get security access token"))) {
      return new BaseException(ErrorCode.FAILED_PRECONDITION, rejectedCredentialsMessage, e);
    }
    if (checkMetadataErrors
        && reasons.contains("Error code 404 trying to get security access token")) {
      return new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "The server's Compute Engine instance has no service account or permission scopes for"
              + " Google Cloud Storage. Contact the server administrator.",
          e);
    }
    // STS converts JSON error responses to OAuthException with isRetryable() == false and no
    // HttpResponseException in the cause chain, so transient OAuth error codes must be classified
    // as UNAVAILABLE before checking Retryable.
    if (reasons.contains("Error code temporarily_unavailable")
        || reasons.contains("Error code server_error")
        || reasons.contains("Error code internal_failure")) {
      return new BaseException(
          ErrorCode.UNAVAILABLE,
          "Could not get a Google access token for this location. Try again.",
          e);
    }
    if (isPermanentFailure(e)) {
      return new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Google refused to issue an access token for the storage credential of this location."
              + " Contact the server administrator.",
          e);
    }
    return new BaseException(
        ErrorCode.UNAVAILABLE,
        "Could not get a Google access token for this location. Try again.",
        e);
  }

  /**
   * Whether the failure is permanent and asking again cannot succeed: a non-retryable 4xx {@link
   * HttpResponseException} (excluding 408 and 429) or a {@link Retryable} marked non-retryable.
   */
  private static boolean isPermanentFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof HttpResponseException response) {
        int status = response.getStatusCode();
        return status >= 400 && status < 500 && status != 408 && status != 429;
      }
      if (cause instanceof Retryable retryable && !retryable.isRetryable()) {
        return true;
      }
    }
    return false;
  }

  private static String causeMessages(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 10; depth++, cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    return messages.toString();
  }

  List<String> resolvePrivilegesToRoles(Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }

  /** Where a generator gets the credentials it downscopes. */
  private interface CredentialsSource {
    GoogleCredentials load() throws IOException;
  }

  private final class ServiceAccountCredentialGenerator implements GcpCredentialGenerator {
    private final CredentialsSource source;
    private final Function<Exception, BaseException> errorMapper;

    private ServiceAccountCredentialGenerator(
        CredentialsSource source, Function<Exception, BaseException> errorMapper) {
      this.source = source;
      this.errorMapper = errorMapper;
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      try {
        GoogleCredentials creds = source.load();
        return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), context).refreshAccessToken();
      } catch (IOException | IllegalStateException e) {
        throw errorMapper.apply(e);
      }
    }
  }
}
