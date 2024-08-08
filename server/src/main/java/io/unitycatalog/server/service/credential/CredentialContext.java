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

  String storageScheme;
  String storageBase;
  Set<Privilege> privileges;
  List<String> locations;
}
