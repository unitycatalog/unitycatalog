package io.unitycatalog.server.persist.model;

import java.util.Date;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;

/** A group and the ids of its member users. */
@Value
@Builder(toBuilder = true)
public class Group {
  UUID id;
  String name;
  String externalId;
  Set<UUID> memberIds;
  Date createdAt;
  Date updatedAt;
}
