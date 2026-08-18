package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Protocol-version negotiation for {@code GET /delta/v1/config}.
 *
 * <p>The client sends {@code protocol-versions}: a comma-separated list of the highest protocol
 * version it supports per major version (e.g. {@code "1.1,2.3"} means it supports 1.0-1.1 and
 * 2.0-2.3). The server picks the highest version both sides support, or rejects the request with
 * {@code INVALID_PARAMETER_VALUE} naming the versions it supports.
 */
final class DeltaProtocolVersionNegotiator {

  /** Protocol versions this server implements, in ascending order. */
  static final List<String> SUPPORTED_VERSIONS = List.of("1.0");

  private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");

  private static final List<Version> SUPPORTED =
      SUPPORTED_VERSIONS.stream()
          .map(v -> Version.tryParse(v).orElseThrow(IllegalArgumentException::new))
          .toList();

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

    Map<Integer, Integer> clientMaxMinorByMajor = new HashMap<>();
    for (String token : clientProtocolVersions.split(",", -1)) {
      Version version =
          Version.tryParse(token.trim())
              .orElseThrow(
                  () ->
                      new BaseException(
                          ErrorCode.INVALID_ARGUMENT,
                          String.format(
                              "Invalid protocol-versions parameter \"%s\": entry \"%s\" is not "
                                  + "of the form <major>.<minor>. %s",
                              clientProtocolVersions, token.trim(), supportedVersionsMessage())));
      clientMaxMinorByMajor.merge(version.major(), version.minor(), Math::max);
    }

    return SUPPORTED.stream()
        .filter(v -> clientMaxMinorByMajor.getOrDefault(v.major(), -1) >= v.minor())
        .max(Comparator.naturalOrder())
        .map(Version::toString)
        .orElseThrow(
            () ->
                new BaseException(
                    ErrorCode.INVALID_ARGUMENT,
                    String.format(
                        "No mutually supported protocol version: client supports \"%s\". %s",
                        clientProtocolVersions.trim(), supportedVersionsMessage())));
  }

  private static String supportedVersionsMessage() {
    return "Server supports protocol versions: " + String.join(", ", SUPPORTED_VERSIONS) + ".";
  }

  private record Version(int major, int minor) implements Comparable<Version> {
    private static final Comparator<Version> ORDER =
        Comparator.comparingInt(Version::major).thenComparingInt(Version::minor);

    static Optional<Version> tryParse(String value) {
      Matcher matcher = VERSION_PATTERN.matcher(value);
      if (!matcher.matches()) {
        return Optional.empty();
      }
      return Optional.of(
          new Version(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))));
    }

    @Override
    public int compareTo(Version other) {
      return ORDER.compare(this, other);
    }

    @Override
    public String toString() {
      return major + "." + minor;
    }
  }
}
