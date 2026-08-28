package io.unitycatalog.server.utils;

import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import lombok.experimental.Delegate;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Test {@link FileIO} that maps one registered cloud prefix onto a local directory, so the server's
 * Iceberg create/commit/load path can run end-to-end without a real cloud backend. A location under
 * {@code cloudPrefix} has that prefix rewritten to {@code localDir} (e.g. prefix {@code
 * s3://bucket/root} + {@code localDir} {@code /tmp/root} maps {@code s3://bucket/root/t/v.json} →
 * {@code /tmp/root/t/v.json}); a location outside the prefix is rejected, so a stray access fails
 * loudly. IO is delegated to {@link SimpleLocalFileIO}.
 *
 * <p>Before any read/write it asserts the vended config carries the scheme's credential keys (see
 * the constructor), so a credential-less cloud FileIO fails loudly. This is lighter than the Spark
 * fakes, which also validate the credential provider wiring and vary credentials per bucket.
 *
 * <p>The returned {@link InputFile}/{@link OutputFile} report the original cloud location, not the
 * mapped local path, so metadata Iceberg reads back keeps its {@code s3://} location.
 */
public class LocalMappingFileIO implements FileIO {

  private final SimpleLocalFileIO delegate = new SimpleLocalFileIO();
  private final NormalizedURL cloudPrefix;
  private final Path localDir;
  private final Map<String, String> config;
  private final boolean writable;

  /**
   * @param cloudPrefix the cloud location prefix this FileIO serves (e.g. {@code s3://bucket/root})
   * @param localDir the local directory {@code cloudPrefix} is rewritten to
   * @param config the config the server vended for the location (from {@code
   *     FileOperations#getFileIOConfig})
   * @param expectedCredentials config-key → expected value; each key the scheme requires must be
   *     present in {@code config}, and where an expected value is given must match it exactly.
   * @param privileges the privileges the server vended for; without {@code UPDATE} this FileIO
   *     rejects writes, mimicking read-only cloud credentials.
   */
  public LocalMappingFileIO(
      NormalizedURL cloudPrefix,
      Path localDir,
      Map<String, String> config,
      Map<String, String> expectedCredentials,
      Set<CredentialContext.Privilege> privileges) {
    this.cloudPrefix = cloudPrefix;
    this.localDir = localDir;
    this.config = config;
    this.writable = privileges.contains(CredentialContext.Privilege.UPDATE);
    validateVendedCredentials(UriScheme.fromURI(cloudPrefix.toUri()), config, expectedCredentials);
  }

  private static void validateVendedCredentials(
      UriScheme scheme, Map<String, String> config, Map<String, String> expected) {
    switch (scheme) {
      case S3 -> {
        requireVendedKey(scheme, config, expected, S3FileIOProperties.ACCESS_KEY_ID);
        requireVendedKey(scheme, config, expected, S3FileIOProperties.SECRET_ACCESS_KEY);
        requireVendedKey(scheme, config, expected, S3FileIOProperties.SESSION_TOKEN);
      }
      case GS -> requireVendedKey(scheme, config, expected, GCPProperties.GCS_OAUTH2_TOKEN);
      case ABFS, ABFSS ->
          requireVendedKeyPrefix(scheme, config, AzureProperties.ADLS_SAS_TOKEN_PREFIX);
      default -> throw new IllegalStateException("Not a cloud scheme: " + scheme);
    }
  }

  /**
   * Requires {@code key} to be present and non-blank, and -- when {@code expected} supplies a value
   * for it -- to equal that value, so the credential is provably the one UC's test vendor produced.
   */
  private static void requireVendedKey(
      UriScheme scheme, Map<String, String> config, Map<String, String> expected, String key) {
    String value = config.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          String.format(
              "No vended credential for %s FileIO: missing/blank '%s' (config keys: %s)",
              scheme, key, config.keySet()));
    }
    String expectedValue = expected.get(key);
    if (expectedValue != null && !expectedValue.equals(value)) {
      throw new IllegalStateException(
          String.format(
              "Credential '%s' for %s FileIO did not match the value vended by UC's test credential"
                  + " vendor (expected '%s'); a present-but-unexpected value suggests it was not"
                  + " produced by UC vending",
              key, scheme, expectedValue));
    }
  }

  private static void requireVendedKeyPrefix(
      UriScheme scheme, Map<String, String> config, String keyPrefix) {
    boolean present =
        config.entrySet().stream()
            .anyMatch(
                e ->
                    e.getKey().startsWith(keyPrefix)
                        && e.getValue() != null
                        && !e.getValue().isBlank());
    if (!present) {
      throw new IllegalStateException(
          String.format(
              "No vended credential for %s FileIO: no non-blank key starting with '%s' (config"
                  + " keys: %s)",
              scheme, keyPrefix, config.keySet()));
    }
  }

  /**
   * Maps a cloud {@code location} under {@code cloudPrefix} to the local file it stands in for,
   * rejecting any location outside the prefix.
   */
  public static Path toLocalPath(String location, NormalizedURL cloudPrefix, Path localDir) {
    String prefix = cloudPrefix.toString();
    if (location.equals(prefix)) {
      return localDir;
    }
    // Match on a path boundary so a sibling (e.g. ".../root2") isn't mistaken for a child of
    // ".../root".
    String base = prefix.endsWith("/") ? prefix : prefix + "/";
    if (!location.startsWith(base)) {
      throw new IllegalStateException(
          String.format("Location %s is not under mapped prefix %s", location, prefix));
    }
    return localDir.resolve(location.substring(base.length()));
  }

  private String toLocalUri(String location) {
    return toLocalPath(location, cloudPrefix, localDir).toUri().toString();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new LocalMappingInputFile(delegate.newInputFile(toLocalUri(path)), path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new LocalMappingInputFile(delegate.newInputFile(toLocalUri(path), length), path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    requireWritable("write");
    return new LocalMappingOutputFile(delegate.newOutputFile(toLocalUri(path)), path);
  }

  @Override
  public void deleteFile(String path) {
    requireWritable("delete");
    delegate.deleteFile(toLocalUri(path));
  }

  /** Fails a mutating op when the FileIO was vended read-only, as read-only cloud creds would. */
  private void requireWritable(String operation) {
    if (!writable) {
      throw new IllegalStateException(
          String.format(
              "Cannot %s through a read-only (SELECT-only) FileIO for %s", operation, cloudPrefix));
    }
  }

  @Override
  public Map<String, String> properties() {
    return config;
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public void close() {
    delegate.close();
  }

  // @Delegate forwards every FileIO method to the wrapped local file; the methods defined below
  // (location, and toInputFile for output) are not generated, since lombok skips a method the class
  // already declares. That lets each wrapper report the original cloud location while doing IO
  // locally.

  /** Delegates all IO to a local {@link InputFile} but reports the original cloud location. */
  private static final class LocalMappingInputFile implements InputFile {
    @Delegate private final InputFile local;
    private final String reportedLocation;

    LocalMappingInputFile(InputFile local, String reportedLocation) {
      this.local = local;
      this.reportedLocation = reportedLocation;
    }

    @Override
    public String location() {
      return reportedLocation;
    }
  }

  /** Delegates all IO to a local {@link OutputFile} but reports the original cloud location. */
  private static final class LocalMappingOutputFile implements OutputFile {
    @Delegate private final OutputFile local;
    private final String reportedLocation;

    LocalMappingOutputFile(OutputFile local, String reportedLocation) {
      this.local = local;
      this.reportedLocation = reportedLocation;
    }

    @Override
    public String location() {
      return reportedLocation;
    }

    @Override
    public InputFile toInputFile() {
      return new LocalMappingInputFile(local.toInputFile(), reportedLocation);
    }
  }
}
