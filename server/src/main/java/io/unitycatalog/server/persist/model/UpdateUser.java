package io.unitycatalog.server.persist.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class UpdateUser {

  private String name;
  private Boolean active;
  private String externalId;
}
