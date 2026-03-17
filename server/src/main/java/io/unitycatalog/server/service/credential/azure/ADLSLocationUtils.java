package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.utils.NormalizedURL;
import java.net.URI;

public class ADLSLocationUtils {
  public record ADLSLocationParts(
      String scheme, String container, String account, String accountName, String path) {}

  public static ADLSLocationParts parseLocation(NormalizedURL location) {
    URI locationUri = location.toUri();

    String[] authorityParts = locationUri.getAuthority().split("@");
    if (authorityParts.length > 1) {
      return new ADLSLocationParts(
          locationUri.getScheme(),
          authorityParts[0],
          authorityParts[1],
          authorityParts[1].split("\\.")[0],
          locationUri.getPath());
    } else {
      return new ADLSLocationParts(
          locationUri.getScheme(),
          null,
          authorityParts[0],
          authorityParts[0].split("\\.")[0],
          locationUri.getPath());
    }
  }
}
