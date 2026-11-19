package io.unitycatalog.hadoop.internal.id;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Uniquely identifies a concrete Hadoop filesystem. Hadoop's canonical filesystem API supports one
 * credential for the entire filesystem, while each credential returned by Unity Catalog is scoped
 * to a storage prefix. Both the credential request ({@link CredId}) and prefix are therefore
 * required to identify the filesystem.
 *
 * <p>{@code prefix} is nullable: a {@code null} prefix keys by {@link CredId} and the storage.
 *
 * <p>For an S3 path, unless the table load vended keys for its bucket on AWS S3, the bucket's S3A
 * settings are part of the identity too: every {@code fs.s3a.} option S3A reads for it, such as the
 * endpoint and region, the transport and signing options, and the engine's own S3A credentials. The
 * same credential request for one bucket name can thus reach two storages, or two engine
 * identities, without sharing a filesystem. They are kept as a hash, so the identity holds no
 * secret. Unless keys were vended for the bucket, as for a vend with only the endpoint or a bucket
 * the table load did not route, the engine signs with its own keys, which may differ per Hadoop
 * user, so the calling user is part of it too.
 *
 * <p>A fixed credential, vended without renewal, is part of the identity too, also as a hash: a
 * filesystem keeps the credential it was created with, so a later table load that vends a new
 * credential gets its own filesystem rather than one whose credential may have expired.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public final class FileSystemCredId {
  // Hadoop's own credential stores, which S3A also reads secrets from.
  private static final String HADOOP_CREDENTIAL_PROVIDER_PATH =
      "hadoop.security.credential.provider.path";

  private final CredId credId;
  private final String prefix;
  private final String s3Storage;
  // The calling Hadoop user for a vend without keys, compared as Hadoop's own file system cache
  // does: two users of one name with different credentials stay apart. Otherwise null.
  private final UserGroupInformation user;
  // A hash of the fixed credential the table load vended, or empty.
  private final String fixedCredential;

  private FileSystemCredId(
      CredId credId,
      String prefix,
      String s3Storage,
      UserGroupInformation user,
      String fixedCredential) {
    this.credId = credId;
    this.prefix = prefix;
    this.s3Storage = s3Storage;
    this.user = user;
    this.fixedCredential = fixedCredential;
  }

  /**
   * Derives the id from {@code conf}: the {@link CredId} for the credential request, the {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_PREFIX_KEY prefix} being served, and a hash of the storage
   * the table load recorded for its buckets.
   */
  public static FileSystemCredId create(Configuration conf) {
    CredId credId = CredId.create(conf);
    String prefix = getCredPrefix(conf);
    return new FileSystemCredId(credId, prefix, vendedStorage(conf), null, "");
  }

  /**
   * The storage the table load recorded for its buckets, so that credentials cached for one storage
   * are not served to a table loaded for another after a credential's storage changed.
   */
  private static String vendedStorage(Configuration conf) {
    Map<String, String> storage = new TreeMap<>();
    conf.getPropsWithPrefix(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX)
        .forEach((bucket, endpoint) -> storage.put("endpoint." + bucket, endpoint));
    conf.getPropsWithPrefix(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX)
        .forEach((bucket, region) -> storage.put("region." + bucket, region));
    conf.getPropsWithPrefix(UCHadoopConfConstants.UC_VENDED_S3_PATH_STYLE_PREFIX)
        .forEach((bucket, pathStyle) -> storage.put("pathStyle." + bucket, pathStyle));
    // A hash, as for the S3A settings, so the id holds no settings and its text form stays short.
    return storage.isEmpty() ? "" : MapIdGenerator.generateId(storage);
  }

  public static FileSystemCredId create(Configuration conf, URI uri, String prefix) {
    CredId credId = CredId.create(conf, () -> new DefaultCredId(uri, conf));
    // Without vended keys the engine signs with its own, which may differ per Hadoop user in ways
    // the S3A settings do not show, such as a provider that reads the user's own keys.
    boolean isSignedByTheEngine = isS3(uri) && !hasVendedKeys(conf, uri);
    return new FileSystemCredId(
        credId,
        prefix,
        s3Storage(conf, uri),
        isSignedByTheEngine ? currentUser() : null,
        conf.get(UCHadoopConfConstants.UC_FIXED_CREDENTIAL_ID_KEY, ""));
  }

  /**
   * @return a hash of the S3A settings for the bucket of {@code uri}, or empty for other schemes
   *     and for AWS S3 (no endpoint set for the bucket, or globally) with vended keys
   */
  static String s3Storage(Configuration conf, URI uri) {
    if (!isS3(uri)) {
      return "";
    }
    String bucketScope = "fs.s3a.bucket." + uri.getAuthority() + ".";
    // AWS S3 with vended keys keeps the key it had, unless the engine routes S3 elsewhere itself.
    if (conf.get(bucketScope + "endpoint") == null
        && conf.get(bucketScope + "endpoint.region") == null
        && conf.get("fs.s3a.endpoint") == null
        && conf.get("fs.s3a.endpoint.region") == null
        && hasVendedKeys(conf, uri)) {
      return "";
    }
    // Every option S3A reads for the bucket, at bucket scope and global, rather than a list kept in
    // step with S3A. Left out: other buckets' scopes, and the initial credentials of each table
    // load (fs.s3a.init.*), which would keep loads of one table from sharing a file system.
    Map<String, String> settings = new TreeMap<>();
    conf.getPropsWithPrefix("fs.s3a.")
        .forEach(
            (option, value) -> {
              if (!option.startsWith("init.")
                  && (!option.startsWith("bucket.")
                      || ("fs.s3a." + option).startsWith(bucketScope))) {
                settings.put("fs.s3a." + option, value);
              }
            });
    settings.put(HADOOP_CREDENTIAL_PROVIDER_PATH, conf.get(HADOOP_CREDENTIAL_PROVIDER_PATH, ""));
    // The bucket itself, as two buckets without options of their own read the same settings.
    settings.put("bucket", uri.getAuthority());
    return MapIdGenerator.generateId(settings);
  }

  private static boolean isS3(URI uri) {
    String scheme = uri.getScheme();
    return uri.getAuthority() != null
        && ("s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme));
  }

  /** Whether the table load recorded that it vended keys for the bucket of {@code uri}. */
  private static boolean hasVendedKeys(Configuration conf, URI uri) {
    return "true"
        .equals(conf.get(UCHadoopConfConstants.UC_VENDED_S3_KEYS_PREFIX + uri.getAuthority()));
  }

  private static UserGroupInformation currentUser() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String getCredPrefix(Configuration conf) {
    return conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY);
  }

  /**
   * The credential prefix being accessed, or {@code null} when the key is scoped by CredId only.
   */
  public String prefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileSystemCredId)) {
      return false;
    }
    FileSystemCredId that = (FileSystemCredId) o;
    return Objects.equals(credId, that.credId)
        && Objects.equals(prefix, that.prefix)
        && s3Storage.equals(that.s3Storage)
        && Objects.equals(user, that.user)
        && fixedCredential.equals(that.fixedCredential);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, prefix, s3Storage, user, fixedCredential);
  }

  @Override
  public String toString() {
    // A short prefix of the storage hash tells keys for two storages apart in logs.
    return "FileSystemCredId{credId="
        + credId
        + ", prefix="
        + prefix
        + (s3Storage.isEmpty()
            ? ""
            : ", s3Storage=" + s3Storage.substring(0, Math.min(8, s3Storage.length())))
        + (user == null ? "" : ", user=" + user.getUserName())
        + "}";
  }
}
