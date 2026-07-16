package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.hadoop.internal.id.CredId;
import org.apache.hadoop.conf.Configuration;

/**
 * Resolves the {@link GenericCredential} that applies to a credential scope.
 */
public final class CredentialResolver {
  private final GenericCredentialFetcher fetcher;

  private CredentialResolver(GenericCredentialFetcher fetcher) {
    this.fetcher = fetcher;
  }

  /** Builds a resolver over the given fetcher. */
  public static CredentialResolver create(GenericCredentialFetcher fetcher) {
    return new CredentialResolver(fetcher);
  }

  /** Builds a resolver from Hadoop configuration via {@link GenericCredentialFetcher#create}. */
  public static CredentialResolver create(Configuration conf) {
    return new CredentialResolver(GenericCredentialFetcher.create(conf));
  }

  /** Fetches credentials and returns the single one that applies to {@code credId}. */
  public GenericCredential select(CredId credId) throws ApiException {
    return CredentialSelector.forCredId(credId).select(fetcher.fetch(), credId);
  }
}
