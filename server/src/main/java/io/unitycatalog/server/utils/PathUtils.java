package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;

public class PathUtils {

  public static PathComponents extractPathComponents(String url) {
    try {
      URI uri = URI.create(url);
      String path = uri.getPath();

      // Remove leading slash
      if (path.startsWith("/")) {
        path = path.substring(1);
      }

      // Expected format: catalog/schema/...
      String[] parts = path.split("/", 3);

      return new PathComponents(
          parts.length > 0 && !parts[0].isBlank() ? parts[0] : null, // catalog
          parts.length > 1 && !parts[1].isBlank() ? parts[1] : null, // schema
          parts.length > 2 && !parts[2].isBlank() ? parts[2] : null // remaining path
          );
    } catch (IllegalArgumentException ex) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, String.format("Invalid path %s", url));
    } catch (NullPointerException ex) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Path cannot be null");
    }
  }

  public record PathComponents(String catalogName, String schemaName, String remainingPath) {}
}
