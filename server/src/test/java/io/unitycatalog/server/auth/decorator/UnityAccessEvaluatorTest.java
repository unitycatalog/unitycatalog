package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizationAction;
import io.unitycatalog.server.auth.AuthorizationProvider;
import io.unitycatalog.server.auth.AuthorizationRequest;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UnityAccessEvaluatorTest {

  @AuthorizeExpression(
      action = AuthorizationAction.CREATE_TABLE,
      value = AuthorizeExpressions.CREATE_TABLE,
      precondition = AuthorizeExpressions.CREATE_TABLE_PRECONDITION)
  private void createTableRule() {}

  @AuthorizeExpression("#principal != null")
  private void authenticationRule() {}

  @Test
  void externalDecisionsReplaceNativePermissionsButCannotSkipPreconditions() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    AuthorizationProvider provider = mock(AuthorizationProvider.class);
    UnityAccessEvaluator evaluator =
        new UnityAccessEvaluator(authorizer, true, Optional.of(provider));
    AuthorizeExpression rule =
        getClass().getDeclaredMethod("createTableRule").getAnnotation(AuthorizeExpression.class);
    AuthorizationRequest.Principal principal =
        new AuthorizationRequest.Principal(UUID.randomUUID(), "user@example.com");
    Map<SecurableType, UUID> resources =
        Map.of(
            SecurableType.CATALOG,
            UUID.randomUUID(),
            SecurableType.SCHEMA,
            UUID.randomUUID(),
            SecurableType.EXTERNAL_LOCATION,
            UUID.randomUUID());
    AuthorizationRequest rejected =
        new AuthorizationRequest(
            principal,
            rule.action(),
            resources,
            Map.of(),
            Map.of("table_type", "EXTERNAL", "no_overlap_with_data_securable", false));
    assertThat(evaluator.evaluate(rule, rejected)).isFalse();
    verifyNoInteractions(provider, authorizer);

    AuthorizationRequest allowed =
        new AuthorizationRequest(
            principal,
            rule.action(),
            resources,
            Map.of(),
            Map.of("table_type", "EXTERNAL", "no_overlap_with_data_securable", true));
    when(provider.authorize(allowed)).thenReturn(true, false);
    assertThat(evaluator.evaluate(rule, allowed)).isTrue();
    assertThat(evaluator.evaluate(rule, allowed)).isFalse();
    verify(provider, times(2)).authorize(allowed);
    verifyNoInteractions(authorizer);

    AuthorizationRequest anonymous =
        new AuthorizationRequest(
            new AuthorizationRequest.Principal(null, null),
            rule.action(),
            resources,
            Map.of(),
            allowed.context());
    assertThat(evaluator.evaluate(rule, anonymous)).isFalse();
    verify(provider, times(2)).authorize(allowed);
  }

  @Test
  void authenticationOnlyRulesRemainLocalWithAnExternalProvider() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    AuthorizationProvider provider = mock(AuthorizationProvider.class);
    UnityAccessEvaluator evaluator =
        new UnityAccessEvaluator(authorizer, true, Optional.of(provider));
    AuthorizeExpression rule =
        getClass().getDeclaredMethod("authenticationRule").getAnnotation(AuthorizeExpression.class);
    assertThat(
            evaluator.evaluate(
                rule,
                new AuthorizationRequest(
                    new AuthorizationRequest.Principal(UUID.randomUUID(), "user@example.com"),
                    rule.action(),
                    Map.of(),
                    Map.of(),
                    Map.of())))
        .isTrue();
    verifyNoInteractions(provider, authorizer);
  }

  @Test
  void evaluateSkipsRefreshWhenExpressionAllows() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#permit", Collections.emptyMap(), Collections.emptyMap()))
        .isTrue();
    verify(authorizer, never()).refreshAuthorizations(anyLong());
  }

  @Test
  void evaluateKeepsDenyWhenRefreshReturnsFalse() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.refreshAuthorizations(anyLong())).thenReturn(false);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#deny", Collections.emptyMap(), Collections.emptyMap()))
        .isFalse();
    verify(authorizer, times(1)).refreshAuthorizations(anyLong());
  }

  @Test
  void evaluateKeepsDenyWhenRefreshThrows() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.refreshAuthorizations(anyLong()))
        .thenThrow(new RuntimeException("reload failed"));
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);

    assertThat(
            evaluator.evaluate(
                UUID.randomUUID(), "#deny", Collections.emptyMap(), Collections.emptyMap()))
        .isFalse();
    verify(authorizer, times(1)).refreshAuthorizations(anyLong());
  }

  @Test
  void evaluateAllowsAfterSuccessfulRefresh() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();

    when(authorizer.authorize(eq(principal), eq(catalog), eq(Privileges.OWNER)))
        .thenReturn(false, true);
    when(authorizer.refreshAuthorizations(anyLong())).thenReturn(true);

    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);

    assertThat(
            evaluator.evaluate(
                principal,
                "#authorize(#principal, #catalog, OWNER)",
                Map.of(SecurableType.CATALOG, catalog),
                Collections.emptyMap()))
        .isTrue();
    verify(authorizer, times(1)).refreshAuthorizations(anyLong());
    verify(authorizer, times(2)).authorize(eq(principal), eq(catalog), eq(Privileges.OWNER));
  }

  @Test
  void evaluateUsesServerPrivilegeChecksSetting() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    for (boolean strictPrivilegeChecks : new boolean[] {true, false}) {
      UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, strictPrivilegeChecks);
      assertThat(
              evaluator.evaluate(
                  UUID.randomUUID(),
                  "#strict_privilege_checks",
                  Map.of(),
                  Map.of("strict_privilege_checks", !strictPrivilegeChecks)))
          .isEqualTo(strictPrivilegeChecks);
    }
  }
}
