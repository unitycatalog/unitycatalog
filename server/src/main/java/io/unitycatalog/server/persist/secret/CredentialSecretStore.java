package io.unitycatalog.server.persist.secret;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.UUID;

/**
 * Keeps the secret values of storage credentials, such as the {@code secret_access_key} of an
 * {@code s3_compatible} credential. The database holds only the stored value that {@link #store}
 * returns: the secret encrypted, or, for a store outside Unity Catalog, a pointer to it.
 *
 * <p>The only store keeps the value in the credential's row, so a value goes with its row. A store
 * outside the database would also need to release values the database no longer holds; add that
 * with such a store.
 */
public interface CredentialSecretStore {

  /**
   * What a secret belongs to.
   *
   * @param credentialId the credential's id, which stays the same across updates
   * @param credentialName the credential's name when the secret is stored, for labeling only
   * @param field the credential field that holds the secret
   */
  record SecretContext(UUID credentialId, String credentialName, String field) {}

  /**
   * @param secret the value to keep
   * @param context what it belongs to
   * @return the stored value to persist in the credential
   */
  String store(String secret, SecretContext context);

  /**
   * @param storedValue a value returned by {@link #store}
   * @param context the context it was stored with; only its id and field need to match
   * @return the secret
   */
  String load(String storedValue, SecretContext context);

  /** The store the server is configured with. */
  static CredentialSecretStore from(ServerProperties serverProperties) {
    return EncryptedCredentialSecretStore.from(serverProperties);
  }
}
