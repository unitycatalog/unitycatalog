package io.unitycatalog.server.service.credential.aws;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An STS generator shared by the requests that use it. One that its cache evicts is closed once the
 * last request using it releases it, so a call in flight never loses its client.
 */
final class SharedStsGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SharedStsGenerator.class);
  private final AwsCredentialGenerator.StsAwsCredentialGenerator generator;
  private int users;
  private boolean isRetired;

  /**
   * @param generator the generator to share, which this closes once it is retired and unused
   */
  SharedStsGenerator(AwsCredentialGenerator.StsAwsCredentialGenerator generator) {
    this.generator = generator;
  }

  /**
   * @return the generator for one request, which must {@link #release} it after use, or empty if it
   *     was retired and the caller should look it up again
   */
  synchronized Optional<AwsCredentialGenerator.StsAwsCredentialGenerator> acquire() {
    if (isRetired) {
      return Optional.empty();
    }
    users++;
    return Optional.of(generator);
  }

  /** Ends a request's use of the generator. */
  void release() {
    boolean shouldClose;
    synchronized (this) {
      users--;
      shouldClose = isRetired && users == 0;
    }
    // Closed outside the lock, so a request that finds it retired does not wait for the close.
    if (shouldClose) {
      closeLogged(generator);
    }
  }

  /** Marks the generator as evicted: no new request gets it, and it closes once unused. */
  void retire() {
    boolean shouldClose;
    synchronized (this) {
      shouldClose = !isRetired && users == 0;
      isRetired = true;
    }
    if (shouldClose) {
      closeLogged(generator);
    }
  }

  /**
   * Closes a generator after a call, or on eviction, so a failed close is logged rather than
   * replacing the call's result.
   *
   * @param generator the generator to close
   */
  static void closeLogged(AwsCredentialGenerator.StsAwsCredentialGenerator generator) {
    try {
      generator.close();
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to close an STS client", e);
    }
  }
}
