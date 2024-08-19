package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.server.model.ResourceType;
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

  private ResourceType type;
  private Source source;
  private String key;
}
