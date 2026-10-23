package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UnityAccessEvaluatorTest {

  @ParameterizedTest
  @CsvSource(
      textBlock =
          """
          , false, false, false, false, false
          , true,  true,  false, false, false
          , true,  true,  false, true,  false
          , true,  false, true,  true,  false
          , false, true,  true,  true,  false
          , false, false, true,  true,  false
          , true,  true,  true,  false, true
          , true,  true,  true,  true,  true
          METASTORE, false, false, false, false, true
          CATALOG,   false, false, false, false, true
          SCHEMA,    false, true,  true,  true,  false
          SCHEMA,    true,  false, false, false, true
          TABLE,     false, false, false, false, false
          TABLE,     false, true,  false, false, false
          TABLE,     true,  false, false, false, false
          TABLE,     true,  true,  false, false, true
          """)
  void getTableRequiresSelectOrAnOwnerRule(
      SecurableType ownedResource,
      boolean useCatalog,
      boolean useSchema,
      boolean select,
      boolean modify,
      boolean expected)
      throws Exception {
    UUID principal = UUID.randomUUID();
    Map<SecurableType, UUID> resources =
        Map.of(
            SecurableType.METASTORE, UUID.randomUUID(),
            SecurableType.CATALOG, UUID.randomUUID(),
            SecurableType.SCHEMA, UUID.randomUUID(),
            SecurableType.TABLE, UUID.randomUUID());
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.authorize(eq(principal), any(UUID.class), eq(Privileges.OWNER)))
        .thenAnswer(
            invocation ->
                ownedResource != null
                    && invocation.getArgument(1).equals(resources.get(ownedResource)));
    when(authorizer.authorize(
            principal, resources.get(SecurableType.CATALOG), Privileges.USE_CATALOG))
        .thenReturn(useCatalog);
    when(authorizer.authorize(
            principal, resources.get(SecurableType.SCHEMA), Privileges.USE_SCHEMA))
        .thenReturn(useSchema);
    when(authorizer.authorize(principal, resources.get(SecurableType.TABLE), Privileges.MODIFY))
        .thenReturn(modify);

    Set<Privileges> tableGrants = EnumSet.noneOf(Privileges.class);
    if (ownedResource == SecurableType.TABLE) {
      tableGrants.add(Privileges.OWNER);
    }
    if (select) {
      tableGrants.add(Privileges.SELECT);
    }
    if (modify) {
      tableGrants.add(Privileges.MODIFY);
    }
    when(authorizer.authorizeAny(
            eq(principal), eq(resources.get(SecurableType.TABLE)), any(Privileges[].class)))
        .thenAnswer(
            invocation ->
                Arrays.stream((Privileges[]) invocation.getRawArguments()[2])
                    .anyMatch(tableGrants::contains));

    for (boolean strictPrivilegeChecks : new boolean[] {true, false}) {
      assertThat(
              new UnityAccessEvaluator(authorizer, strictPrivilegeChecks)
                  .evaluate(principal, AuthorizeExpressions.GET_TABLE, resources, Map.of()))
          .as("strict=%s", strictPrivilegeChecks)
          .isEqualTo(expected || (!strictPrivilegeChecks && useCatalog && useSchema && modify));
    }
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
