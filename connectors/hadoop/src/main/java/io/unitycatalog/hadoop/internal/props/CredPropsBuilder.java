package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CloudType;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Builds the cloud-provider specific Hadoop configuration credential properties. */
public abstract class CredPropsBuilder {
  /** CredScopedFileSystem implementation classes, swapped in when cred-scoped FS is enabled. */
  protected static final String CRED_SCOPED_FS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem";

  protected static final String CRED_SCOPED_AFS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFs";

  private final Configuration hadoopConf;
  private final HashMap<String, String> props = new HashMap<>();

  private boolean renewCredEnabled;
  private GenericCredential initialCredential;

  protected CredPropsBuilder(Configuration hadoopConf) {
    Preconditions.checkNotNull(hadoopConf, "hadoopConf is required");
    this.hadoopConf = hadoopConf;
  }

  /** Returns a builder for {@code cloudType}, seeded with that cloud's static base properties. */
  public static CredPropsBuilder forCloud(CloudType cloudType, Configuration hadoopConf) {
    switch (cloudType) {
      case S3:
        return new S3CredPropsBuilder(hadoopConf);
      case GCS:
        return new GcsCredPropsBuilder(hadoopConf);
      case ABFS:
        return new AbfsCredPropsBuilder(hadoopConf);
    }
    throw new IllegalStateException("Unhandled cloud type: " + cloudType);
  }

  /**
   * Records whether renewal is enabled and, when it is, writes the wiring the vended token provider
   * needs to renew the credential: the provider-class keys, the catalog uri, the auth configs, the
   * credential-scope identity, and the engine versions. The fixed path writes none of this.
   */
  public CredPropsBuilder renewCredEnabled(
      boolean renewCredEnabled,
      String catalogUri,
      TokenProvider tokenProvider,
      CredId credId,
      Map<String, String> appVersions) {
    this.renewCredEnabled = renewCredEnabled;
    if (renewCredEnabled) {
      applyVendedProviderKeys();
      set(UCHadoopConfConstants.UC_URI_KEY, catalogUri);
      // Only 'fs.*' properties propagate to the FileSystem, so prefix the auth configs.
      tokenProvider
          .configs()
          .forEach((key, value) -> set(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
      credId.props().forEach(this::set);
      appVersions.forEach(
          (key, value) -> set(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + key, value));
    }
    return this;
  }

  /** When enabled, writes the cloud filesystem-impl overrides that install CredScopedFileSystem. */
  public CredPropsBuilder credScopedFsEnabled(boolean credScopedFsEnabled) {
    if (credScopedFsEnabled) {
      writeImplOverrides();
    }
    return this;
  }

  /** Records the fetched credential whose secrets are written by {@link #build()}. */
  public CredPropsBuilder initialCredential(GenericCredential initialCredential) {
    this.initialCredential = initialCredential;
    return this;
  }

  public Map<String, String> build() {
    Preconditions.checkNotNull(initialCredential, "initialCredential is required");
    if (renewCredEnabled) {
      writeRenewableCredKeys(initialCredential);
    } else {
      writeFixedCredKeys(initialCredential);
    }
    return Collections.unmodifiableMap(new HashMap<>(props));
  }

  protected CredPropsBuilder set(String key, String value) {
    props.put(key, value);
    return this;
  }

  /**
   * Saves the current value of {@code key} from the Hadoop conf (falling back to {@code
   * defaultOriginal}) under {@code key + ".original"}, then overrides {@code key} with {@code
   * newValue}. The saved {@code .original} value is the side channel a reader uses to restore the
   * real delegate implementation after the wrapper has been installed.
   */
  protected CredPropsBuilder saveAndOverride(String key, String defaultOriginal, String newValue) {
    props.put(key + ".original", hadoopConf.get(key, defaultOriginal));
    props.put(key, newValue);
    return this;
  }

  /** Cloud-specific filesystem-impl overrides that install CredScopedFileSystem. */
  protected abstract void writeImplOverrides();

  /** Cloud-specific key naming the vended token provider (renewable path only). */
  protected abstract void applyVendedProviderKeys();

  /** Writes the renewable-path credential secrets (the {@code init.*} keys) for this cloud. */
  protected abstract void writeRenewableCredKeys(GenericCredential cred);

  /** Writes the fixed-path credential secrets for this cloud. */
  protected abstract void writeFixedCredKeys(GenericCredential cred);
}
