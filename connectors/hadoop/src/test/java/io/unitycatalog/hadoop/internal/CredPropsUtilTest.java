package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link CredPropsUtil} helper behavior that is neither cloud-specific property emission
 * (see {@link CredPropsBaseTest} and its subclasses) nor credential caching (see {@link
 * CredPropsCacheTest}).
 */
class CredPropsUtilTest {

  @Test
  void credContextIdVariesByCatalogUriSchemeAndAuth() {
    TokenProvider auth = tokenProvider();
    String base = CredPropsUtil.credContextId("http://uc", "s3", auth);

    assertThat(CredPropsUtil.credContextId("http://uc", "s3", auth)).isEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc-2", "s3", auth)).isNotEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc", "gs", auth)).isNotEqualTo(base);
    assertThat(
            CredPropsUtil.credContextId(
                "http://uc", "s3", TokenProvider.create(Map.of("type", "static", "token", "x"))))
        .isNotEqualTo(base);
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
