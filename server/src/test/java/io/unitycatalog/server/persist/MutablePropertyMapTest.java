package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.utils.Constants;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link MutablePropertyMap}'s diff-flush branches: unchanged-value no-op,
 * value-changed update, inserted key, removed key. Uses a mocked {@link Session} so the assertions
 * can verify exactly which rows get persisted / merged / removed without starting a database.
 */
public class MutablePropertyMapTest {

  @Test
  public void asMapReflectsLoadedState() {
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(prop("k1", "v1"), prop("k2", "v2")));
    assertThat(map.asMap()).containsEntry("k1", "v1").containsEntry("k2", "v2").hasSize(2);
    assertThat(map.get("k1")).isEqualTo("v1");
  }

  @Test
  public void putReplacesExistingValue() {
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(prop("k", "v1")));
    map.put("k", "v2");
    assertThat(map.get("k")).isEqualTo("v2");
  }

  @Test
  public void removeDropsKeyFromView() {
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(prop("k", "v")));
    map.remove("k");
    assertThat(map.asMap()).doesNotContainKey("k");
  }

  @Test
  public void removeAllDropsAllListedKeys() {
    MutablePropertyMap map =
        MutablePropertyMap.wrap(List.of(prop("a", "1"), prop("b", "2"), prop("c", "3")));
    map.removeAll(List.of("a", "c"));
    assertThat(map.asMap()).doesNotContainKeys("a", "c").containsEntry("b", "2");
  }

  @Test
  public void removeMatchingPrefixDropsMatchingKeys() {
    MutablePropertyMap map =
        MutablePropertyMap.wrap(
            List.of(
                prop("delta.feature.a", "supported"),
                prop("delta.feature.b", "supported"),
                prop("user.custom", "v")));
    map.removeMatchingPrefix("delta.feature.");
    assertThat(map.asMap())
        .doesNotContainKey("delta.feature.a")
        .doesNotContainKey("delta.feature.b")
        .containsEntry("user.custom", "v");
  }

  @Test
  public void flushPersistsInsertsUpdatesAndDeletionsOnly() {
    PropertyDAO kept = prop("kept", "unchanged");
    PropertyDAO modified = prop("modified", "old");
    PropertyDAO removed = prop("removed", "gone");
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(kept, modified, removed));

    // Edits: leave 'kept' alone, change 'modified', delete 'removed', insert 'added'.
    map.put("modified", "new");
    map.remove("removed");
    map.put("added", "v");

    Session session = mock(Session.class);
    UUID tableId = UUID.randomUUID();
    map.flush(session, tableId);

    // 'removed' is the only delete.
    verify(session, times(1)).remove(removed);
    verify(session, never()).remove(kept);
    verify(session, never()).remove(modified);

    // 'modified' is merged with the new value set.
    assertThat(modified.getValue()).isEqualTo("new");
    verify(session, times(1)).merge(modified);
    // 'kept' is a no-op -- no merge, no persist.
    verify(session, never()).merge(kept);

    // 'added' is persisted as a fresh row.
    ArgumentCaptor<PropertyDAO> persisted = ArgumentCaptor.forClass(PropertyDAO.class);
    verify(session, times(1)).persist(persisted.capture());
    PropertyDAO inserted = persisted.getValue();
    assertThat(inserted.getKey()).isEqualTo("added");
    assertThat(inserted.getValue()).isEqualTo("v");
    assertThat(inserted.getEntityId()).isEqualTo(tableId);
    assertThat(inserted.getEntityType()).isEqualTo(Constants.TABLE);
  }

  @Test
  public void flushIsNoOpWhenNothingChanged() {
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(prop("k", "v")));
    Session session = mock(Session.class);
    map.flush(session, UUID.randomUUID());
    verify(session, never()).persist(any());
    verify(session, never()).merge(any());
    verify(session, never()).remove(any());
  }

  @Test
  public void flushOnEmptyStateInsertsEverything() {
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of());
    map.putAll(Map.of("a", "1", "b", "2"));
    Session session = mock(Session.class);
    UUID tableId = UUID.randomUUID();
    map.flush(session, tableId);
    verify(session, atLeastOnce()).persist(any(PropertyDAO.class));
    verify(session, never()).remove(any());
  }

  @Test
  public void putThenRemoveIsNoOpAgainstLoadedState() {
    // Adding then removing a key that was never in the loaded state must NOT emit a DELETE.
    MutablePropertyMap map = MutablePropertyMap.wrap(List.of(prop("k", "v")));
    map.put("ephemeral", "x");
    map.remove("ephemeral");
    Session session = mock(Session.class);
    map.flush(session, UUID.randomUUID());
    verify(session, never()).persist(any());
    verify(session, never()).remove(any());
  }

  private static PropertyDAO prop(String key, String value) {
    PropertyDAO dao =
        PropertyDAO.builder()
            .id(UUID.randomUUID())
            .entityId(UUID.randomUUID())
            .entityType(Constants.TABLE)
            .key(key)
            .value(value)
            .build();
    // Stubbing equals via mock would be overkill; the DAO has @EqualsAndHashCode.
    return dao;
  }
}
