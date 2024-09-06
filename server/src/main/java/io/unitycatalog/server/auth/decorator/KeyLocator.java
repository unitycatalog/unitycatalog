package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.server.model.SecurableType;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class KeyLocator {
  public enum Source {
    SYSTEM,
    PARAM,
    PAYLOAD;
  }

  private SecurableType type;
  private Source source;
  private String key;
}
