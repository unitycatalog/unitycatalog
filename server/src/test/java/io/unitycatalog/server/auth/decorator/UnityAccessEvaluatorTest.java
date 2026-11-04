package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.MODIFY;
import static io.unitycatalog.server.persist.model.Privileges.READ_METADATA;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static io.unitycatalog.server.persist.model.Privileges.USE_CATALOG;
import static io.unitycatalog.server.persist.model.Privileges.USE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class UnityAccessEvaluatorTest {

  @ParameterizedTest
  @EnumSource(SecurableType.class)
  void preservesResourceTypeForEachDecisionMethod(SecurableType type) throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    when(authorizer.authorize(principal, type, resource, Privileges.OWNER)).thenReturn(true);
    when(authorizer.authorizeAny(principal, type, resource, Privileges.OWNER, Privileges.SELECT))
        .thenReturn(true);
    when(authorizer.authorizeAll(principal, type, resource, Privileges.SELECT, Privileges.MODIFY))
        .thenReturn(true);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);
    String variable = "#" + type.name().toLowerCase();

    assertThat(
            evaluator.evaluate(
                principal,
                "#authorize(#principal, "
                    + variable
                    + ", OWNER) && "
                    + "#authorizeAny(#principal, "
                    + variable
                    + ", OWNER, SELECT) && "
                    + "#authorizeAll(#principal, "
                    + variable
                    + ", SELECT, MODIFY)",
                Map.of(type, resource),
                Map.of()))
        .isTrue();
    verify(authorizer).authorize(principal, type, resource, Privileges.OWNER);
    verify(authorizer).authorizeAny(principal, type, resource, Privileges.OWNER, Privileges.SELECT);
    verify(authorizer)
        .authorizeAll(principal, type, resource, Privileges.SELECT, Privileges.MODIFY);
  }

  @Test
  void missingResourcesRemainNullAndShortCircuit() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    Map<SecurableType, UUID> resources = new HashMap<>();
    resources.put(TABLE, null);

    assertThat(
            new UnityAccessEvaluator(authorizer, true)
                .evaluate(
                    UUID.randomUUID(),
                    "(#table == null || #authorize(#principal, #table, OWNER)) && "
                        + "(#external_location == null || "
                        + "#authorizeAny(#principal, #external_location, OWNER, READ_FILES))",
                    resources,
                    Map.of()))
        .isTrue();
    verifyNoInteractions(authorizer);
  }

  @Test
  void listFilteringPreservesItemResourceType() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID table = UUID.randomUUID();
    when(authorizer.authorize(principal, TABLE, table, Privileges.OWNER)).thenReturn(true);
    List<TableInfo> items = new ArrayList<>(List.of(new TableInfo().tableId(table.toString())));

    new ResultFilter(
            new UnityAccessEvaluator(authorizer, true),
            principal,
            "#authorize(#principal, #table, OWNER)",
            Map.of(),
            Map.of(),
            mock(KeyMapper.class))
        .filter(TABLE, items);

    assertThat(items).hasSize(1);
    verify(authorizer).authorize(principal, TABLE, table, Privileges.OWNER);
  }

  @ParameterizedTest
  @MethodSource("credentialAccessCombinations")
  void tableCredentialsRequireUsageAndDataAccess(
      boolean catalogUsage, boolean schemaUsage, boolean tableRead, boolean tableWrite)
      throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID catalog = UUID.randomUUID();
    UUID schema = UUID.randomUUID();
    UUID table = UUID.randomUUID();
    when(authorizer.authorizeAny(
            principal, CATALOG, catalog, Privileges.OWNER, Privileges.USE_CATALOG))
        .thenReturn(catalogUsage);
    when(authorizer.authorizeAny(
            principal, SCHEMA, schema, Privileges.OWNER, Privileges.USE_SCHEMA))
        .thenReturn(schemaUsage);
    when(authorizer.authorizeAny(principal, TABLE, table, Privileges.OWNER, Privileges.SELECT))
        .thenReturn(tableRead);
    when(authorizer.authorizeAll(principal, TABLE, table, Privileges.SELECT, Privileges.MODIFY))
        .thenReturn(tableRead && tableWrite);
    when(authorizer.authorize(principal, CATALOG, catalog, Privileges.USE_CATALOG))
        .thenReturn(catalogUsage);
    when(authorizer.authorize(principal, SCHEMA, schema, Privileges.USE_SCHEMA))
        .thenReturn(schemaUsage);
    when(authorizer.authorize(principal, TABLE, table, Privileges.MODIFY)).thenReturn(tableWrite);
    when(authorizer.authorizeAny(
            principal, TABLE, table, Privileges.OWNER, Privileges.SELECT, Privileges.MODIFY))
        .thenReturn(tableRead || tableWrite);

    Map<SecurableType, UUID> resources =
        Map.of(
            SecurableType.METASTORE,
            UUID.randomUUID(),
            SecurableType.CATALOG,
            catalog,
            SecurableType.SCHEMA,
            schema,
            SecurableType.TABLE,
            table);
    boolean canRead = catalogUsage && schemaUsage && tableRead;
    for (boolean strictPrivilegeChecks : new boolean[] {true, false}) {
      UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, strictPrivilegeChecks);
      assertThat(
              evaluator.evaluate(
                  principal,
                  AuthorizeExpressions.VEND_TABLE_CREDENTIAL,
                  resources,
                  Map.of("operation", "READ")))
          .isEqualTo(canRead);
      assertThat(
              evaluator.evaluate(
                  principal,
                  AuthorizeExpressions.VEND_TABLE_CREDENTIAL,
                  resources,
                  Map.of("operation", "READ_WRITE")))
          .isEqualTo(canRead && tableWrite);
      assertThat(
              evaluator.evaluate(
                  principal,
                  AuthorizeExpressions.VEND_ICEBERG_TABLE_CREDENTIAL,
                  resources,
                  Map.of()))
          .isEqualTo(
              strictPrivilegeChecks
                  ? canRead
                  : catalogUsage && schemaUsage && (tableRead || tableWrite));
    }
  }

  static Stream<Arguments> credentialAccessCombinations() {
    List<Arguments> combinations = new ArrayList<>();
    for (boolean catalogUsage : new boolean[] {false, true}) {
      for (boolean schemaUsage : new boolean[] {false, true}) {
        for (boolean tableRead : new boolean[] {false, true}) {
          for (boolean tableWrite : new boolean[] {false, true}) {
            combinations.add(Arguments.of(catalogUsage, schemaUsage, tableRead, tableWrite));
          }
        }
      }
    }
    return combinations.stream();
  }

  @Test
  void getTableRequiresReadAccessOrAnOwnerRule() throws Exception {
    assertGetTable(null, Set.of(), false, false);
    assertGetTable(null, Set.of(USE_CATALOG, USE_SCHEMA), false, false);
    // MODIFY without SELECT passes only with strict checks off.
    assertGetTable(null, Set.of(USE_CATALOG, USE_SCHEMA, MODIFY), false, true);
    assertGetTable(null, Set.of(USE_CATALOG, SELECT, MODIFY), false, false);
    assertGetTable(null, Set.of(USE_SCHEMA, SELECT, MODIFY), false, false);
    assertGetTable(null, Set.of(SELECT, MODIFY), false, false);
    assertGetTable(null, Set.of(USE_CATALOG, USE_SCHEMA, SELECT), true, true);
    assertGetTable(null, Set.of(USE_CATALOG, USE_SCHEMA, SELECT, MODIFY), true, true);
    assertGetTable(METASTORE, Set.of(), true, true);
    assertGetTable(CATALOG, Set.of(), true, true);
    assertGetTable(SCHEMA, Set.of(USE_SCHEMA, SELECT, MODIFY), false, false);
    assertGetTable(SCHEMA, Set.of(USE_CATALOG), true, true);
    assertGetTable(TABLE, Set.of(), false, false);
    assertGetTable(TABLE, Set.of(USE_SCHEMA), false, false);
    assertGetTable(TABLE, Set.of(USE_CATALOG), false, false);
    assertGetTable(TABLE, Set.of(USE_CATALOG, USE_SCHEMA), true, true);

    for (SecurableType scope : List.of(METASTORE, CATALOG)) {
      assertGetTable(null, scope, Set.of(), true, true);
    }
    assertGetTable(null, SCHEMA, Set.of(), false, false);
    assertGetTable(null, SCHEMA, Set.of(USE_SCHEMA), false, false);
    assertGetTable(null, SCHEMA, Set.of(USE_CATALOG), true, true);
    assertGetTable(null, TABLE, Set.of(), false, false);
    assertGetTable(null, TABLE, Set.of(USE_SCHEMA), false, false);
    assertGetTable(null, TABLE, Set.of(USE_CATALOG), false, false);
    assertGetTable(null, TABLE, Set.of(USE_CATALOG, USE_SCHEMA), true, true);
  }

  private void assertGetTable(
      SecurableType ownedResource,
      Set<Privileges> grants,
      boolean expectedStrict,
      boolean expectedLegacy)
      throws Exception {
    assertGetTable(ownedResource, null, grants, expectedStrict, expectedLegacy);
  }

  private void assertGetTable(
      SecurableType ownedResource,
      SecurableType metadataResource,
      Set<Privileges> grants,
      boolean expectedStrict,
      boolean expectedLegacy)
      throws Exception {
    UUID principal = UUID.randomUUID();
    Map<SecurableType, UUID> resources =
        Map.of(
            SecurableType.METASTORE, UUID.randomUUID(),
            SecurableType.CATALOG, UUID.randomUUID(),
            SecurableType.SCHEMA, UUID.randomUUID(),
            SecurableType.TABLE, UUID.randomUUID());
    Map<UUID, Set<Privileges>> assignments = new HashMap<>();
    for (UUID resource : resources.values()) {
      assignments.put(resource, EnumSet.noneOf(Privileges.class));
    }
    for (Privileges grant : grants) {
      SecurableType type =
          switch (grant) {
            case USE_CATALOG -> CATALOG;
            case USE_SCHEMA -> SCHEMA;
            default -> TABLE;
          };
      assignments.get(resources.get(type)).add(grant);
    }
    if (ownedResource != null) {
      assignments.get(resources.get(ownedResource)).add(Privileges.OWNER);
    }
    if (metadataResource != null) {
      assignments.get(resources.get(metadataResource)).add(READ_METADATA);
    }

    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    when(authorizer.authorize(
            eq(principal), any(SecurableType.class), any(UUID.class), any(Privileges.class)))
        .thenAnswer(
            invocation ->
                assignments.get(invocation.getArgument(2)).contains(invocation.getArgument(3)));
    when(authorizer.authorizeAny(
            eq(principal), any(SecurableType.class), any(UUID.class), any(Privileges[].class)))
        .thenAnswer(
            invocation ->
                Arrays.stream((Privileges[]) invocation.getRawArguments()[3])
                    .anyMatch(assignments.get(invocation.getArgument(2))::contains));

    for (boolean strictPrivilegeChecks : new boolean[] {true, false}) {
      UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, strictPrivilegeChecks);
      assertThat(evaluator.evaluate(principal, AuthorizeExpressions.GET_TABLE, resources, Map.of()))
          .as(
              "owner=%s, metadata=%s, grants=%s, strict=%s",
              ownedResource, metadataResource, grants, strictPrivilegeChecks)
          .isEqualTo(strictPrivilegeChecks ? expectedStrict : expectedLegacy);
      if (metadataResource != null) {
        assertThat(
                evaluator.evaluate(
                    principal,
                    AuthorizeExpressions.VEND_ICEBERG_TABLE_CREDENTIAL,
                    resources,
                    Map.of()))
            .as(
                "metadata=%s, grants=%s, strict=%s",
                metadataResource, grants, strictPrivilegeChecks)
            .isFalse();
      }
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

    when(authorizer.authorize(eq(principal), eq(CATALOG), eq(catalog), eq(Privileges.OWNER)))
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
    verify(authorizer, times(2))
        .authorize(eq(principal), eq(CATALOG), eq(catalog), eq(Privileges.OWNER));
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
