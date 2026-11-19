package io.unitycatalog.server.service.credential.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

public class SharedStsGeneratorTest {
  private final AwsCredentialGenerator.StsAwsCredentialGenerator generator =
      mock(AwsCredentialGenerator.StsAwsCredentialGenerator.class);
  private final SharedStsGenerator shared = new SharedStsGenerator(generator);

  @Test
  public void testRetiredGeneratorClosesAfterItsLastUser() {
    // Two requests hold it when the cache evicts it: it closes only once both are done.
    assertThat(shared.acquire()).contains(generator);
    assertThat(shared.acquire()).contains(generator);
    shared.retire();
    verify(generator, never()).close();
    // A request after the eviction does not get it.
    assertThat(shared.acquire()).isEmpty();
    shared.release();
    verify(generator, never()).close();
    shared.release();
    verify(generator, times(1)).close();
  }

  @Test
  public void testFailedCloseDoesNotFailTheCall() {
    // The last release runs in the call's finally block, so a failed close must not replace its
    // result.
    doThrow(new IllegalStateException("close failed")).when(generator).close();
    assertThat(shared.acquire()).contains(generator);
    shared.retire();
    assertThatCode(shared::release).doesNotThrowAnyException();
    verify(generator, times(1)).close();
  }

  @Test
  public void testUnusedGeneratorClosesWhenRetired() {
    assertThat(shared.acquire()).contains(generator);
    shared.release();
    verify(generator, never()).close();
    shared.retire();
    verify(generator, times(1)).close();
  }
}
