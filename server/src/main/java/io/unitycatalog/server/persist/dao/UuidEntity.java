package io.unitycatalog.server.persist.dao;

import java.util.UUID;

/**
 * An entity whose {@code @Id} is a UUID. The by-key helpers in {@code RepositoryUtils} name a row
 * by this id plus the key the request used.
 */
public interface UuidEntity {
  UUID getId();
}
