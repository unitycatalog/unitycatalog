package io.unitycatalog.server.service.credential;

import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CredentialContext {
  public enum Privilege {
    SELECT,
    UPDATE
  }

  private String storageScheme;
  private String storageBase;
  private Set<Privilege> privileges;
  private List<String> locations;
}
