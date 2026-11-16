package io.unitycatalog.server.persist.model;

import io.unitycatalog.server.utils.IdentityUtils;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CreateUser {

  private String name;
  private String email;
  private String externalId;
  private Boolean active;
  private String pictureUrl;
  @Builder.Default private String owner = IdentityUtils.ADMIN_OWNER;
}
