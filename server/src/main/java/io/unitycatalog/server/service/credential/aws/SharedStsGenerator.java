package io.unitycatalog.server.service.credential.aws;

import java.util.Optional;

/**
 * An STS generator shared by the requests that use it. One that its cache evicts is closed once the
 * last request using it releases it, so a call in flight never loses its client.
 */
final class SharedStsGenerator {
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
  synchronized void release() {
    users--;
    closeIfRetiredAndUnused();
  }

  /** Marks the generator as evicted: no new request gets it, and it closes once unused. */
  synchronized void retire() {
    isRetired = true;
    closeIfRetiredAndUnused();
  }

  private void closeIfRetiredAndUnused() {
    if (isRetired && users == 0) {
      generator.close();
    }
  }
}
