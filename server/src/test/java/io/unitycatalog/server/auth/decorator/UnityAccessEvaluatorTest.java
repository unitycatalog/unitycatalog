package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UnityAccessEvaluatorTest {

  @Test
  void evaluateSkipsRefreshWhenExpressionAllows() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#permit", Collections.emptyMap(), Collections.emptyMap()))
        .isTrue();
    verify(authorizer, never()).refreshAuthorizations();
  }

  @Test
  void evaluateKeepsDenyWhenRefreshReturnsFalse() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.refreshAuthorizations()).thenReturn(false);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#deny", Collections.emptyMap(), Collections.emptyMap()))
        .isFalse();
    verify(authorizer, times(1)).refreshAuthorizations();
  }

  @Test
  void evaluateKeepsDenyWhenRefreshThrows() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.refreshAuthorizations()).thenThrow(new RuntimeException("reload failed"));
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#deny", Collections.emptyMap(), Collections.emptyMap()))
        .isFalse();
    verify(authorizer, times(1)).refreshAuthorizations();
  }

  @Test
  void evaluateAllowsAfterSuccessfulRefresh() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();

    when(authorizer.authorize(eq(principal), eq(catalog), eq(Privileges.OWNER)))
        .thenReturn(false, true);
    when(authorizer.refreshAuthorizations()).thenReturn(true);

    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer);

    assertThat(
            evaluator.evaluate(
                principal,
                "#authorize(#principal, #catalog, OWNER)",
                Map.of(SecurableType.CATALOG, catalog),
                Collections.emptyMap()))
        .isTrue();
    verify(authorizer, times(1)).refreshAuthorizations();
    verify(authorizer, times(2)).authorize(eq(principal), eq(catalog), eq(Privileges.OWNER));
  }
}
