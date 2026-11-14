package io.unitycatalog.hadoop.internal.id;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

/**
 * Uniquely identifies a concrete Hadoop filesystem. Hadoop's canonical filesystem API supports one
 * credential for the entire filesystem, while each credential returned by Unity Catalog is scoped
 * to a storage prefix. Both the credential request ({@link CredId}) and prefix are therefore
 * required to identify the filesystem.
 *
 * <p>{@code prefix} is nullable: a {@code null} prefix keys purely by {@link CredId}.
 *
 * <p>On S3-compatible storage (a bucket with a bucket-scoped S3 endpoint set) the S3A settings that
 * route and sign its requests are part of the identity too: the endpoint and region, and the
 * engine's own S3A credentials, which serve a vend without keys. The same credential request for
 * one bucket name can thus reach two storages, or two engine identities, without sharing a
 * filesystem. They are kept as a hash, so the identity holds no secret.
 *
 * <p><b>Internal API — not for external use. May change without notice.</b>
 */
public final class FileSystemCredId {
  // The S3A options that route and sign a bucket's requests, read at bucket scope, then global:
  // the endpoint and region, the keys, the providers, the credential stores S3A reads secrets from,
  // and the role it assumes.
  private static final List<String> S3_STORAGE_OPTIONS =
      Arrays.asList(
          "endpoint",
          "endpoint.region",
          "access.key",
          "secret.key",
          "session.token",
          "aws.credentials.provider",
          "security.credential.provider.path",
          "assumed.role.arn",
          "assumed.role.credentials.provider",
          "assumed.role.session.name",
          "assumed.role.policy",
          "assumed.role.sts.endpoint",
          "assumed.role.sts.endpoint.region");

  // Hadoop's own credential stores, which S3A also reads secrets from.
  private static final String HADOOP_CREDENTIAL_PROVIDER_PATH =
      "hadoop.security.credential.provider.path";

  private final CredId credId;
  private final String prefix;
  private final String s3Storage;

  private FileSystemCredId(CredId credId, String prefix, String s3Storage) {
    this.credId = credId;
    this.prefix = prefix;
    this.s3Storage = s3Storage;
  }

  /**
   * Derives the id from {@code conf}: the {@link CredId} for the credential request plus the {@link
   * UCHadoopConfConstants#UC_CREDENTIAL_PREFIX_KEY prefix} being served.
   */
  public static FileSystemCredId create(Configuration conf) {
    CredId credId = CredId.create(conf);
    String prefix = getCredPrefix(conf);
    return new FileSystemCredId(credId, prefix, "");
  }

  public static FileSystemCredId create(Configuration conf, URI uri, String prefix) {
    CredId credId = CredId.create(conf, () -> new DefaultCredId(uri, conf));
    return new FileSystemCredId(credId, prefix, s3Storage(conf, uri));
  }

  /**
   * @return a hash of the S3A settings for the bucket of {@code uri} on S3-compatible storage, or
   *     empty for AWS S3 (no endpoint set for the bucket) and other schemes
   */
  static String s3Storage(Configuration conf, URI uri) {
    String scheme = uri.getScheme();
    if (uri.getAuthority() == null || !("s3".equals(scheme) || "s3a".equals(scheme))) {
      return "";
    }
    String bucketScope = "fs.s3a.bucket." + uri.getAuthority() + ".";
    if (conf.get(bucketScope + "endpoint") == null) {
      return "";
    }
    StringBuilder settings = new StringBuilder();
    for (String option : S3_STORAGE_OPTIONS) {
      String value = conf.get(bucketScope + option, conf.get("fs.s3a." + option, ""));
      settings.append(option).append('=').append(value).append('\n');
    }
    settings
        .append(HADOOP_CREDENTIAL_PROVIDER_PATH)
        .append('=')
        .append(conf.get(HADOOP_CREDENTIAL_PROVIDER_PATH, ""));
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(settings.toString().getBytes(StandardCharsets.UTF_8));
      return new BigInteger(1, digest).toString(16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
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
        && s3Storage.equals(that.s3Storage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, prefix, s3Storage);
  }

  @Override
  public String toString() {
    return "FileSystemCredId{credId=" + credId + ", prefix=" + prefix + "}";
  }
}
