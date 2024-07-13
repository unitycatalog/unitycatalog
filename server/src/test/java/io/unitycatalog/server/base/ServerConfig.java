package io.unitycatalog.server.base;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ServerConfig {
  private String serverUrl;
  private String authToken;

  public ServerConfig(String serverUrl, String authToken) {
    this.serverUrl = serverUrl;
    this.authToken = authToken;
  }
}
