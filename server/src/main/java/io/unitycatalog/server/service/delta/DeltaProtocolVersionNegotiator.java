package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Protocol-version negotiation for {@code GET /delta/v1/config}.
 *
 * <p>The client sends {@code protocol-versions}: a comma-separated list of the highest protocol
 * version it supports per major version (e.g. {@code "1.1,2.3"} means it supports 1.0-1.1 and
 * 2.0-2.3). The server picks the highest version both sides support, or rejects the request with
 * {@code INVALID_PARAMETER_VALUE} naming the versions it supports.
 *
 * <p>The server implements exactly one protocol version today, so negotiation reduces to: every
 * entry is well-formed, and some entry covers {@link #CURRENT_VERSION}. If a second version is
 * added, this class is the single place that must learn to compare minors within a major.
 */
final class DeltaProtocolVersionNegotiator {

  /** The only protocol version this server implements. */
  static final String CURRENT_VERSION = "1.0";

  private static final int CURRENT_MAJOR = 1;

  // Bounded digit counts keep Integer.parseInt safe; no real protocol version comes close.
  private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d{1,4})\\.(\\d{1,5})");

  private DeltaProtocolVersionNegotiator() {}

  /**
   * Returns the highest protocol version supported by both the client and this server.
   *
   * @param clientProtocolVersions the raw {@code protocol-versions} query parameter
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} if the parameter is missing,
   *     malformed, or shares no version with the server
   */
  static String negotiate(String clientProtocolVersions) {
    if (clientProtocolVersions == null || clientProtocolVersions.isBlank()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Must supply the protocol-versions parameter: a comma-separated list of the highest "
              + "protocol versions the client supports per major version (e.g. \"1.0\"). "
              + supportedVersionsMessage());
    }

    boolean clientSupportsCurrentVersion = false;
    for (String token : clientProtocolVersions.split(",", -1)) {
      String entry = token.trim();
      Matcher matcher = VERSION_PATTERN.matcher(entry);
      if (!matcher.matches()) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format(
                "Invalid protocol-versions parameter \"%s\": entry \"%s\" is not of the form "
                    + "<major>.<minor>. %s",
                clientProtocolVersions, entry, supportedVersionsMessage()));
      }
      // The entry is the client's highest supported minor for that major, so any 1.x entry
      // covers 1.0 (minors are non-negative).
      clientSupportsCurrentVersion |= Integer.parseInt(matcher.group(1)) == CURRENT_MAJOR;
    }

    if (!clientSupportsCurrentVersion) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "No mutually supported protocol version: client supports \"%s\". %s",
              clientProtocolVersions.trim(), supportedVersionsMessage()));
    }
    return CURRENT_VERSION;
  }

  private static String supportedVersionsMessage() {
    return "Server supports protocol versions: " + CURRENT_VERSION + ".";
  }
}
