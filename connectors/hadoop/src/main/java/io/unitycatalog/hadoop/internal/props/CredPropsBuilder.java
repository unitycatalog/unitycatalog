package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.auth.TokenProvider;
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

  private final HashMap<String, String> props = new HashMap<>();

  /** Returns a builder for {@code cloudType}, seeded with that cloud's static base properties. */
  public static CredPropsBuilder forCloud(CloudType cloudType) {
    switch (cloudType) {
      case S3:
        return new S3CredPropsBuilder();
      case GCS:
        return new GcsCredPropsBuilder();
      case ABFS:
        return new AbfsCredPropsBuilder();
    }
    throw new IllegalStateException("Unhandled cloud type: " + cloudType);
  }

  protected CredPropsBuilder set(String key, String value) {
    props.put(key, value);
    return this;
  }

  /**
   * Saves the current value of {@code key} from {@code conf} (falling back to {@code
   * defaultOriginal}) under {@code key + ".original"}, then overrides {@code key} with {@code
   * newValue}. The saved {@code .original} value is the side channel a reader uses to restore the
   * real delegate implementation after the wrapper has been installed.
   */
  protected CredPropsBuilder saveAndOverride(
      Configuration conf, String key, String defaultOriginal, String newValue) {
    props.put(key + ".original", conf.get(key, defaultOriginal));
    props.put(key, newValue);
    return this;
  }

  /** Writes each cloud's conf-derived base properties. Always applied. */
  public final CredPropsBuilder applyBaseConf(Configuration conf) {
    writeBaseConf(conf);
    return this;
  }

  /**
   * Installs the filesystem-impl overrides that route the scheme through {@code
   * CredScopedFileSystem}, but only when cred-scoped FS is enabled; otherwise a no-op.
   */
  public final CredPropsBuilder applyImplOverrides(
      boolean credScopedFsEnabled, Configuration conf) {
    if (credScopedFsEnabled) {
      writeImplOverrides(conf);
    }
    return this;
  }

  /**
   * Writes the wiring the vended token provider needs to renew the credential: the provider-class
   * keys, the catalog uri, the auth configs, the credential-scope identity, and the engine
   * versions. Only the renewable path carries this wiring; the fixed path writes nothing here.
   */
  public final CredPropsBuilder applyRenewalContext(
      boolean renewable,
      String catalogUri,
      TokenProvider tokenProvider,
      CredId credId,
      Map<String, String> appVersions) {
    if (!renewable) {
      return this;
    }
    applyVendedProviderKeys();
    set(UCHadoopConfConstants.UC_URI_KEY, catalogUri);
    // Only 'fs.*' properties propagate to the FileSystem, so prefix the auth configs.
    tokenProvider
        .configs()
        .forEach((key, value) -> set(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
    credId.props().forEach(this::set);
    appVersions.forEach(
        (key, value) -> set(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + key, value));
    return this;
  }

  /** Cloud-specific conf-derived base properties. Default none; overridden when needed. */
  protected void writeBaseConf(Configuration conf) {}

  /** Cloud-specific filesystem-impl overrides installed. */
  protected abstract void writeImplOverrides(Configuration conf);

  /** Cloud-specific keys naming the vended token provider (renewable path only). */
  protected abstract void applyVendedProviderKeys();

  /** Writes a single credential's secrets for this cloud. */
  public abstract CredPropsBuilder writeCredKeys(boolean renewable, GenericCredential cred);

  public Map<String, String> build() {
    return Collections.unmodifiableMap(new HashMap<>(props));
  }
}
