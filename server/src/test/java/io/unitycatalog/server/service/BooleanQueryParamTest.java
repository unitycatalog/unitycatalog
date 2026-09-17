package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.sendRawDelete;
import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * The query parameters the OpenAPI spec types as booleans, over raw HTTP.
 *
 * <p>The generated client cannot send the values under test -- its {@code parameters_to_url_query}
 * lowercases a boolean -- so these requests go straight at the endpoints. {@code True} / {@code
 * False} is what a hand-written Python client sends, and every case here that carries one is
 * answered 400 "Can't convert 'True' to type 'Boolean'" by a handler that binds the parameter as
 * {@code Optional<Boolean>}.
 */
public class BooleanQueryParamTest extends BaseTableCRUDTestEnv {

  private static final String BASE_PATH = "/api/2.1/unity-catalog";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Every spelling that must be read, plus the empty value Armeria treats as absent. */
  private static final List<String> SPELLINGS =
      List.of("True", "False", "TRUE", "false", "true", "1", "0", "");

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  /**
   * Each {@code force} parameter, against a resource that does not exist. The handler's own
   * not-found answer is the proof that the value converted -- the request reached the handler --
   * and it leaves no state behind, so one server serves every spelling.
   */
  @Test
  @SneakyThrows
  public void forceIsReadOnEveryDelete() {
    List<String> paths =
        List.of(
            "/catalogs/no_such_catalog",
            "/schemas/no_such_catalog.no_such_schema",
            "/functions/no_such_catalog.no_such_schema.no_such_function",
            "/models/no_such_catalog.no_such_schema.no_such_model",
            "/credentials/no_such_credential",
            "/external-locations/no_such_external_location");
    for (String path : paths) {
      for (String spelling : SPELLINGS) {
        HttpResponse<String> response =
            sendRawDelete(serverConfig, BASE_PATH + path + "?force=" + spelling);
        assertThat(response.statusCode())
            .as("DELETE %s?force=%s answered: %s", path, spelling, response.body())
            .isEqualTo(404);
      }
    }
  }

  /** The listing parameters: {@code include_browse} on volumes, {@code omit_*} on tables. */
  @Test
  @SneakyThrows
  public void listingParamsAreReadOnEverySpelling() {
    VolumeInfo volume =
        new VolumesApi(TestUtils.createApiClient(serverConfig))
            .createVolume(
                new CreateVolumeRequestContent()
                    .name(TestUtils.VOLUME_NAME)
                    .catalogName(TestUtils.CATALOG_NAME)
                    .schemaName(TestUtils.SCHEMA_NAME)
                    .volumeType(VolumeType.EXTERNAL)
                    .storageLocation(testDirectoryRoot.toString()));
    for (String spelling : SPELLINGS) {
      assertOk(sendRawGet(serverConfig, tablesPath(spelling)));
      assertOk(sendRawGet(serverConfig, volumesPath(spelling)));
      // getVolume documents include_browse without acting on it, so the volume it answers with is
      // all the proof available that the value converted.
      assertOk(
          sendRawGet(
              serverConfig,
              BASE_PATH + "/volumes/" + volume.getFullName() + "?include_browse=" + spelling));
    }
  }

  /**
   * A capitalized spelling is read as its value, not merely accepted: it decides what the table
   * listing carries, and whether a catalog delete cascades.
   */
  @Test
  @SneakyThrows
  public void capitalizedSpellingIsReadAsItsValue() {
    createAndVerifyExternalTable();

    JsonNode omitted = listedTable("True");
    assertThat(omitted.path("columns")).isEmpty();
    assertThat(omitted.path("properties")).isEmpty();
    JsonNode kept = listedTable("False");
    assertThat(kept.path("columns")).hasSize(COLUMNS.size());
    assertThat(kept.path("properties")).hasSize(TestUtils.PROPERTIES.size());

    // The catalog holds a schema, so force decides the outcome: False refuses the delete and True
    // cascades it.
    String catalogPath = BASE_PATH + "/catalogs/" + TestUtils.CATALOG_NAME;
    assertHttpApiException(
        sendRawDelete(serverConfig, catalogPath + "?force=False"),
        ErrorCode.FAILED_PRECONDITION,
        "Cannot delete catalog with schemas");
    assertOk(sendRawDelete(serverConfig, catalogPath + "?force=True"));
    assertThat(sendRawGet(serverConfig, catalogPath).statusCode()).isEqualTo(404);
  }

  /** A value that is not a boolean at all stays a 400, naming the parameter and the value. */
  @Test
  @SneakyThrows
  public void valueThatIsNotABooleanIsRejected() {
    assertHttpApiException(
        sendRawDelete(serverConfig, BASE_PATH + "/catalogs/no_such_catalog?force=yes"),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid force: yes. It must be true or false.");
    assertHttpApiException(
        sendRawGet(serverConfig, tablesPath("maybe")),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid omit_properties: maybe. It must be true or false.");
    assertHttpApiException(
        sendRawGet(serverConfig, volumesPath("2")),
        ErrorCode.INVALID_ARGUMENT,
        "Invalid include_browse: 2. It must be true or false.");
  }

  private String tablesPath(String omit) {
    return BASE_PATH
        + "/tables?catalog_name="
        + TestUtils.CATALOG_NAME
        + "&schema_name="
        + TestUtils.SCHEMA_NAME
        + "&omit_properties="
        + omit
        + "&omit_columns="
        + omit;
  }

  private String volumesPath(String includeBrowse) {
    return BASE_PATH
        + "/volumes?catalog_name="
        + TestUtils.CATALOG_NAME
        + "&schema_name="
        + TestUtils.SCHEMA_NAME
        + "&include_browse="
        + includeBrowse;
  }

  /** The one table in the schema, as the listing renders it for the given {@code omit_*} value. */
  @SneakyThrows
  private JsonNode listedTable(String omit) {
    HttpResponse<String> response = sendRawGet(serverConfig, tablesPath(omit));
    assertOk(response);
    JsonNode tables = MAPPER.readTree(response.body()).path("tables");
    assertThat(tables).hasSize(1);
    return tables.path(0);
  }

  private void assertOk(HttpResponse<String> response) {
    assertThat(response.statusCode()).as("answered: %s", response.body()).isEqualTo(200);
  }
}
