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
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.util.Arrays;
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

  /**
   * Records the credential identity whose props ({@code fs.unitycatalog.path}, table id, …) are
   * always written by {@link #build()}, including when renewal is disabled.
   */
  public CredPropsBuilder credId(CredId credId) {
    Preconditions.checkNotNull(credId, "credId is required");
    this.credId = credId;
    return this;
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
    // Set the props when credential scoped filesystem enabled.
    if (credScopedFsEnabled) {
      setCredScopedFsKeys();
    }

    // Path/table identity is independent of renewal: analyzers skip re-vending by matching these
    // keys even when only static access keys are present.
    if (credId != null) {
      credId.props().forEach(this::set);
    }

    // Set the props when renewal credential enabled.
    if (renewCredEnabled) {
      setVendedProviderKeys();
      set(UCHadoopConfConstants.UC_URI_KEY, catalogUri);
      // Only 'fs.*' properties propagate to the FileSystem, so prefix the auth configs.
      tokenProvider
          .configs()
          .forEach((key, value) -> set(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
      appVersions.forEach(
          (key, value) -> set(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + key, value));
    }

    // Set the props for initial credentials.
    Preconditions.checkState(
        initialCredentials != null && !initialCredentials.isEmpty(),
        "Initial credentials cannot be null or empty");
    if (initialCredentials.size() == 1) {
      // Seed the initial credential only in the common single-credential case. In the uncommon
      // multiple-credential case, skip this optimization to keep the code and configuration simple.
      GenericCredential credential = initialCredentials.get(0);
      if (renewCredEnabled) {
        setInitRenewableCredKeys(credential);
      } else {
        Map<String, String> fixedCredKeys = initFixedCredKeys(credential);
        fixedCredKeys.forEach(this::set);
        // Nothing renews a fixed credential, so each one needs its own credential-scoped file
        // system: a cached one keeps the credential of the table load that created it.
        if (credScopedFsEnabled) {
          set(
              UCHadoopConfConstants.UC_FIXED_CREDENTIAL_ID_KEY,
              MapIdGenerator.generateId(fixedCredKeys));
        }
      }
    }

    setS3EndpointKeys(initialCredentials);

    // Set the props for initial credential prefixes.
    List<String> credPrefixes =
        initialCredentials.stream()
            .map(GenericCredential::prefix)
            .filter(prefix -> prefix != null && !prefix.isEmpty())
            .collect(Collectors.toList());
    if (!credPrefixes.isEmpty()) {
      set(
          UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
          String.join(",", CredentialUtil.encodeCredPrefixes(credPrefixes)));
    }

    return Collections.unmodifiableMap(new HashMap<>(props));
  }

  protected CredPropsBuilder set(String key, String value) {
    props.put(key, value);
    return this;
  }

  /** The engine's Hadoop configuration the properties are built for. */
  protected Configuration hadoopConf() {
    return hadoopConf;
  }

  /** Drops a key set earlier in this build. */
  protected void unset(String key) {
    props.remove(key);
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

  /**
   * Whether the engine's configuration sets {@code key} itself: a value counts as the engine's own
   * unless every recorded source is {@code core-default.xml}, which holds Hadoop's own defaults
   * such as S3A's default provider list and Hadoop 3.5's GCS conflict-check default.
   */
  protected boolean engineSets(String key) {
    if (hadoopConf().get(key) == null) {
      return false;
    }
    // No recorded source means we cannot show the value came from defaults, so respect it.
    String[] sources = hadoopConf().getPropertySources(key);
    return sources == null || !Arrays.stream(sources).allMatch("core-default.xml"::equals);
  }

  protected boolean isCredScopedFsEnabled() {
    return credScopedFsEnabled;
  }

  /** Whether credential renewal is enabled, so the vended provider serves the credentials. */
  protected boolean isRenewCredEnabled() {
    return renewCredEnabled;
  }

  /**
   * Sets the S3 endpoint keys for the initial credentials. No-op unless the cloud supports storage
   * endpoints other than its own.
   *
   * @param credentials the initial credentials
   */
  protected void setS3EndpointKeys(List<GenericCredential> credentials) {}

  /** Cloud-specific filesystem-impl overrides that install CredScopedFileSystem. */
  protected abstract void setCredScopedFsKeys();

  /** Cloud-specific key naming the vended token provider (renewable path only). */
  protected abstract void setVendedProviderKeys();

  /** Writes the renewable-path credential secrets (the {@code init.*} keys) for this cloud. */
  protected abstract void setInitRenewableCredKeys(GenericCredential cred);

  /**
   * Returns the fixed-path credential secrets for this cloud, which {@link #build()} sets, or an
   * empty map when the credential is not served through them.
   */
  protected abstract Map<String, String> initFixedCredKeys(GenericCredential cred);
}
