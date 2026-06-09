package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

final class ShallowCloneCredScope {

  private ShallowCloneCredScope() {}

  static Configuration selectConf(URI uri, Configuration conf) {
    String cloneLocation = conf.get(UCHadoopConfConstants.UC_SHALLOW_CLONE_CLONE_LOCATION_KEY);
    if (cloneLocation == null || cloneLocation.isEmpty()) {
      return conf;
    }
    if (uri == null || isUnder(uri.toString(), cloneLocation)) {
      return conf;
    }

    String prefix = UCHadoopConfConstants.UC_SHALLOW_CLONE_SOURCE_PREFIX;
    Configuration effective = new Configuration(conf);
    boolean activated = false;
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.length() > prefix.length() && key.startsWith(prefix)) {
        effective.set(key.substring(prefix.length()), entry.getValue());
        activated = true;
      }
    }
    return activated ? effective : conf;
  }

  static boolean isUnder(String path, String base) {
    String p = normalize(path);
    String b = normalize(base);
    if (b.isEmpty()) {
      return false;
    }
    return p.equals(b) || p.startsWith(b + "/");
  }

  private static String normalize(String uri) {
    String s = uri;
    int schemeSep = s.indexOf("://");
    if (schemeSep >= 0) {
      s = s.substring(schemeSep + 3);
    }
    int end = s.length();
    while (end > 1 && s.charAt(end - 1) == '/') {
      end--;
    }
    return s.substring(0, end);
  }
}
