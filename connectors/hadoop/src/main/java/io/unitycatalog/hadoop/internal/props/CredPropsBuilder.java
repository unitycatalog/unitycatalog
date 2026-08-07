package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CloudType;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem;
import io.unitycatalog.hadoop.internal.fs.CredScopedFs;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;

/** Builds the cloud-provider specific Hadoop configuration credential properties. */
public abstract class CredPropsBuilder {
  /** CredScopedFileSystem implementation classes, swapped in when cred-scoped FS is enabled. */
  protected static final String CRED_SCOPED_FS_CLASS = CredScopedFileSystem.class.getName();

  protected static final String CRED_SCOPED_AFS_CLASS = CredScopedFs.class.getName();

  private final Configuration hadoopConf;
  private final HashMap<String, String> props = new HashMap<>();

  private boolean renewCredEnabled;
  private boolean credScopedFsEnabled;
  private String catalogUri;
  private TokenProvider tokenProvider;
  private CredId credId;
  private Map<String, String> appVersions;
  private List<GenericCredential> initialCredentials;

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

  /** Records the configuration needed to enable credential renewal. */
  public CredPropsBuilder enableRenewCred(
      String catalogUri,
      TokenProvider tokenProvider,
      CredId credId,
      Map<String, String> appVersions) {
    Preconditions.checkNotNull(catalogUri, "catalogUri is required");
    Preconditions.checkNotNull(tokenProvider, "tokenProvider is required");
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(appVersions, "appVersions is required");
    this.renewCredEnabled = true;
    this.catalogUri = catalogUri;
    this.tokenProvider = tokenProvider;
    this.credId = credId;
    this.appVersions = appVersions;
    return this;
  }

  /** Records whether the cloud filesystem implementation overrides should be enabled. */
  public CredPropsBuilder credScopedFsEnabled(boolean credScopedFsEnabled) {
    this.credScopedFsEnabled = credScopedFsEnabled;
    return this;
  }

  /** Records the fetched credentials used by {@link #build()}. */
  public CredPropsBuilder initialCredentials(List<GenericCredential> initialCredentials) {
    this.initialCredentials = initialCredentials;
    return this;
  }

  public Map<String, String> build() {
    Preconditions.checkState(
        initialCredentials != null && !initialCredentials.isEmpty(),
        "Initial credentials cannot be null or empty");

    if (credScopedFsEnabled) {
      setCredScopedFsKeys();
    }
    if (renewCredEnabled) {
      setVendedProviderKeys();
      set(UCHadoopConfConstants.UC_URI_KEY, catalogUri);
      // Only 'fs.*' properties propagate to the FileSystem, so prefix the auth configs.
      tokenProvider
          .configs()
          .forEach((key, value) -> set(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
      credId.props().forEach(this::set);
      appVersions.forEach(
          (key, value) -> set(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + key, value));
    }
    List<String> credPrefixes =
        initialCredentials.stream()
            .map(GenericCredential::prefix)
            .filter(prefix -> prefix != null && !prefix.isEmpty())
            .collect(Collectors.toList());

    if (initialCredentials.size() == 1) {
      GenericCredential credential = initialCredentials.get(0);
      if (renewCredEnabled) {
        setInitRenewableCredKeys(credential);
      } else {
        setInitFixedCredKeys(credential);
      }
    } else {
      // Do not set initial credentials when there are multiple credentials to minimize the
      // number of key that need to be encoded. Instead, token providers fetch credentials
      // at file system initialization time.
      Preconditions.checkState(
          credScopedFsEnabled,
          "%s credentials were vended but the credential-scoped filesystem is disabled.",
          initialCredentials.size());
      Preconditions.checkState(
          renewCredEnabled,
          "%s credentials were vended but credential renewal is disabled.",
          initialCredentials.size());
      // Since there are multiple credentials, each credential's prefix must be non-null
      // and non-empty to differentiate between credentials.
      Preconditions.checkArgument(
          credPrefixes.size() == initialCredentials.size(),
          "Credential prefixes cannot be null or empty when multiple credentials are vended");
    }

    if (!credPrefixes.isEmpty()) {
      String commaSeparatedPrefixes =
          String.join(",", CredentialUtil.encodeCredPrefixes(credPrefixes));
      set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY, commaSeparatedPrefixes);
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
  protected abstract void setCredScopedFsKeys();

  /** Cloud-specific key naming the vended token provider (renewable path only). */
  protected abstract void setVendedProviderKeys();

  /** Writes the renewable-path credential secrets (the {@code init.*} keys) for this cloud. */
  protected abstract void setInitRenewableCredKeys(GenericCredential cred);

  /** Writes the fixed-path credential secrets for this cloud. */
  protected abstract void setInitFixedCredKeys(GenericCredential cred);
}
