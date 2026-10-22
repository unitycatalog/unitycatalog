package io.unitycatalog.server.persist.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CreateUser {

  private String name;
  private String email;
  private String externalId;
  private String issuer;
  private String subject;
  private Boolean active;
  private String pictureUrl;
}
