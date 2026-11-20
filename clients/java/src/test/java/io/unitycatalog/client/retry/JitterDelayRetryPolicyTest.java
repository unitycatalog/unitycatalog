package io.unitycatalog.client.retry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class JitterDelayRetryPolicyTest {

  @Test
  public void testAllowRetryOnlyBeforeTheLastAttempt() {
    RetryPolicy policy = JitterDelayRetryPolicy.builder().maxAttempts(3).build();

    assertThat(policy.allowRetry(1)).isTrue();
    assertThat(policy.allowRetry(2)).isTrue();
    assertThat(policy.allowRetry(3)).isFalse();
  }
}
