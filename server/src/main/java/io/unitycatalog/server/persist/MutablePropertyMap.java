package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.utils.Constants;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hibernate.Session;

/**
 * Session-scoped mutable view of a table's properties.
 *
 * <p>The typical property-mutation path through a single request used to issue one {@link
 * PropertyRepository#findProperties} call per action (set-properties, remove-properties,
 * set-protocol, set-/remove-domain-metadata, ...), plus one more during response assembly -- five
 * or more DB round-trips for a non-trivial update, since Hibernate does not cache HQL query
 * results. This class loads the properties once, accumulates in-memory edits, and flushes only the
 * diff at the end of the transaction.
 *
 * <p>Typical lifecycle:
 *
 * <ol>
 *   <li>{@link #load(Session, UUID)} at the start of the transaction
 *   <li>{@link #put}, {@link #putAll}, {@link #remove}, {@link #removeAll}, {@link
 *       #removeMatchingPrefix} during the apply phase -- all purely in memory
 *   <li>{@link #flush(Session, UUID)} once at the end, which issues one insert / update / delete
 *       per changed key (skipping keys whose value is unchanged)
 *   <li>{@link #asMap()} is then safe to reuse for response assembly without a re-read
 * </ol>
 */
public final class MutablePropertyMap {

  private final Map<String, PropertyDAO> loadedByKey;
  private final Map<String, String> current;

  private MutablePropertyMap(List<PropertyDAO> loaded) {
    this.loadedByKey =
        loaded.stream().collect(Collectors.toMap(PropertyDAO::getKey, Function.identity()));
    this.current = new HashMap<>();
    loaded.forEach(p -> current.put(p.getKey(), p.getValue()));
  }

  /** Load the current property set for {@code tableId} and wrap it for mutation. */
  public static MutablePropertyMap load(Session session, UUID tableId) {
    return new MutablePropertyMap(
        PropertyRepository.findProperties(session, tableId, Constants.TABLE));
  }

  /** Wrap an already-loaded list of properties (visible for unit tests). */
  public static MutablePropertyMap wrap(List<PropertyDAO> loaded) {
    return new MutablePropertyMap(loaded);
  }

  /** Read-only view of the current state (loaded rows plus pending edits). */
  public Map<String, String> asMap() {
    return Collections.unmodifiableMap(current);
  }

  public String get(String key) {
    return current.get(key);
  }

  public void put(String key, String value) {
    current.put(key, value);
  }

  public void putAll(Map<String, String> entries) {
    current.putAll(entries);
  }

  public void remove(String key) {
    current.remove(key);
  }

  public void removeAll(Collection<String> keys) {
    keys.forEach(current::remove);
  }

  /**
   * Remove every key starting with {@code keyPrefix}. Used by {@code set-protocol} to replace all
   * {@code delta.feature.*} entries in one call.
   */
  public void removeMatchingPrefix(String keyPrefix) {
    current.keySet().removeIf(k -> k.startsWith(keyPrefix));
  }

  /**
   * Persist only the keys that changed since {@link #load}: delete keys removed, insert keys added,
   * update keys whose value differs. No-op for keys whose value is unchanged.
   */
  public void flush(Session session, UUID tableId) {
    // Deletions: loaded keys no longer present in the current view.
    for (Map.Entry<String, PropertyDAO> entry : loadedByKey.entrySet()) {
      if (!current.containsKey(entry.getKey())) {
        session.remove(entry.getValue());
      }
    }
    // Inserts and updates.
    for (Map.Entry<String, String> entry : current.entrySet()) {
      PropertyDAO existing = loadedByKey.get(entry.getKey());
      if (existing == null) {
        session.persist(
            PropertyDAO.builder()
                .entityId(tableId)
                .entityType(Constants.TABLE)
                .key(entry.getKey())
                .value(entry.getValue())
                .build());
      } else if (!Objects.equals(existing.getValue(), entry.getValue())) {
        existing.setValue(entry.getValue());
        session.merge(existing);
      }
    }
  }
}
