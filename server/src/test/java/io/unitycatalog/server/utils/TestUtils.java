package io.unitycatalog.server.utils;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.base.ServerConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {
  public static final String CATALOG_NAME = "uc_testcatalog";
  public static final String SCHEMA_NAME = "uc_testschema";
  public static final String CATALOG_NAME2 = "uc_testcatalog2";
  public static final String SCHEMA_NAME2 = "uc_testschema2";
  public static final String TABLE_NAME = "uc_testtable";
  public static final String STORAGE_LOCATION = "/tmp/stagingLocation";
  public static final String VOLUME_NAME = "uc_testvolume";
  public static final String FUNCTION_NAME = "uc_testfunction";
  public static final String MODEL_NAME = "uc_testmodel";
  public static final String MODEL_NEW_NAME = "uc_newtestmodel";
  public static final String SCHEMA_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME;
  public static final String SCHEMA_NEW_NAME = "uc_newtestschema";
  public static final String SCHEMA_NEW_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NEW_NAME;
  public static final String SCHEMA_NEW_COMMENT = "new test comment";
  public static final String TABLE_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
  public static final String VOLUME_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NAME;
  public static final String FUNCTION_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME;
  public static final String MODEL_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + MODEL_NAME;
  public static final String MODEL_NEW_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + MODEL_NEW_NAME;
  public static final String COMMENT = "test comment";
  public static final String CATALOG_NEW_NAME = "uc_newtestcatalog";
  public static final String CATALOG_NEW_COMMENT = "new test comment";
  public static final String MODEL_NEW_COMMENT = "new test model comment";
  public static final String VOLUME_NEW_NAME = "uc_newtestvolume";
  public static final String VOLUME_NEW_FULL_NAME =
      CATALOG_NAME + "." + SCHEMA_NAME + "." + VOLUME_NEW_NAME;
  public static final String MV_COMMENT = "model version comment";
  public static final String MV_SOURCE = "model version source";
  public static final String MV_RUNID = "model version runId";
  public static final String MV_SOURCE2 = "model version source 2";
  public static final String MV_RUNID2 = "model version runId 2";

  public static final Map<String, String> PROPERTIES =
      new HashMap<>(Map.of("prop1", "value1", "prop2", "value2"));
  public static final Map<String, String> NEW_PROPERTIES =
      new HashMap<>(Map.of("prop2", "value22", "prop3", "value33"));
  public static final String COMMON_ENTITY_NAME = "zz_uc_common_entity_name";

  public static int getRandomPort() {
    return (int) (Math.random() * 1000) + 9000;
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
          request -> request.header("Authorization", "Bearer " + serverConfig.getAuthToken()));
    }
    return apiClient;
  }
}
