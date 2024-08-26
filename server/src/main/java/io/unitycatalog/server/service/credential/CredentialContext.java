package io.unitycatalog.server.service.credential;

import java.net.URI;
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
  // This is a list of locations to be a little future-proofing when a table could
  // have more than 1 location where files belonging to table are located
  private List<String> locations;

  public static CredentialContext create(URI locationURI, Set<Privilege> privileges) {
    return CredentialContext.builder()
        .privileges(privileges)
        .storageScheme(locationURI.getScheme())
        .storageBase(locationURI.getScheme() + "://" + locationURI.getAuthority())
        .locations(List.of(locationURI.toString()))
        .build();
  }
}
