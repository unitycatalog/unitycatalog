package io.unitycatalog.server.utils;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.base.ServerConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TestUtils {
  public static final String CATALOG_NAME = "uc_testcatalog";
  public static final String SCHEMA_NAME = "uc_testschema";
  public static final String TABLE_NAME = "uc_testtable";
  public static final String STORAGE_LOCATION = "/tmp/stagingLocation";
  public static final String VOLUME_NAME = "uc_testvolume";
  public static final String FUNCTION_NAME = "uc_testfunction";
  public static final String SCHEMA_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME;
  public static final String SCHEMA_NEW_NAME = "uc_newtestschema";
  public static final String SCHEMA_NEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NEW_NAME;
  public static final String SCHEMA_NEW_COMMENT = "new test comment";
  public static final String TABLE_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
  public static final String VOLUME_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NAME;
  public static final String FUNCTION_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME;
  public static final String COMMENT = "test comment";
  public static final String CATALOG_NEW_NAME = "uc_newtestcatalog";
  public static final String CATALOG_NEW_COMMENT = "new test comment";
  public static final String VOLUME_NEW_NAME = "uc_newtestvolume";
  public static final String VOLUME_NEW_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NEW_NAME;
  public static final Map<String, String> PROPERTIES =
      new HashMap<>(Map.of("prop1", "value1", "prop2", "value2"));
  public static final Map<String, String> NEW_PROPERTIES =
      new HashMap<>(Map.of("prop2", "value22", "prop3", "value33"));

  public static int getRandomPort() {
    return (int) (Math.random() * 1000) + 9000;
  }

  public static <T> int getSize(Iterable<T> iterable) {
    int size = 0;
    Iterator<T> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      size++;
    }
    return size;
  }

  public static <T> boolean contains(Iterable<T> iterable, T element, Function<T, Boolean> equals) {
    Iterator<T> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      if (equals != null) {
        if (equals.apply(iterator.next())) {
          return true;
        }
      } else {
        if (iterator.next().equals(element)) {
          return true;
        }
      }
    }
    return false;
  }

  // Generic function to convert Iterable to List using Stream API
  public static <T> List<T> toList(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
  }

  public static ApiClient createApiClient(ServerConfig serverConfig) {
    ApiClient apiClient = new ApiClient();
    URI uri = URI.create(serverConfig.getServerUrl());
    int port = uri.getPort();
    apiClient.setHost(uri.getHost());
    apiClient.setPort(port);
    apiClient.setScheme(uri.getScheme());
    if (serverConfig.getAuthToken() != null && !serverConfig.getAuthToken().isEmpty()) {
      apiClient.setRequestInterceptor(
          request -> {
            request.header("Authorization", "Bearer " + serverConfig.getAuthToken());
          });
    }
    return apiClient;
  }
}
