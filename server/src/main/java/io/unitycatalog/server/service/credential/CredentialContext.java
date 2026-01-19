package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
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

  private UriScheme storageScheme;
  private NormalizedURL storageBase;
  private Set<Privilege> privileges;
  // This is a list of locations to be a little future-proofing when a table could
  // have more than 1 location where files belonging to table are located
  private List<NormalizedURL> locations;

  public static CredentialContext create(URI locationURI, Set<Privilege> privileges) {
    NormalizedURL location = NormalizedURL.from(locationURI.toString());
    return CredentialContext.builder()
        .privileges(privileges)
        .storageScheme(UriScheme.fromURI(locationURI))
        .storageBase(location.getStorageBase())
        .locations(List.of(location))
        .build();
  }
}
