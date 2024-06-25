package io.unitycatalog.server.iceberg;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.auth.AuthToken;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.HibernateUtil;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.RESTObjectMapper;
import io.unitycatalog.server.utils.TestUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class IcebergRestCatalogTest extends BaseServerTest {

  protected CatalogOperations catalogOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private WebClient client;

  @Before
  public void setUp() {
    super.setUp();
    String uri = serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg";
    String token = serverConfig.getAuthToken();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    tableOperations = new SdkTableOperations(TestUtils.createApiClient(serverConfig));
    client = WebClient
      .builder(uri)
      .auth(AuthToken.ofOAuth2(token))
      .build();
    cleanUp();
  }

    protected void cleanUp() {
        try {
          if (catalogOperations.getCatalog(TestUtils.CATALOG_NAME) != null) {
            catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME);
          }
        } catch (Exception e) {
          // Ignore
        }
        try {
        if (schemaOperations.getSchema(TestUtils.CATALOG_NAME +"." +TestUtils.SCHEMA_NAME) != null) {
            schemaOperations.deleteSchema(TestUtils.CATALOG_NAME+"."+TestUtils.SCHEMA_NAME);
        }
        } catch (Exception e) {
        // Ignore
        }
        try {
        if (tableOperations.getTable(TestUtils.CATALOG_NAME+ "." + TestUtils.SCHEMA_NAME + "." +TestUtils.TABLE_NAME) != null) {
            tableOperations.deleteTable(TestUtils.CATALOG_NAME+ "." + TestUtils.SCHEMA_NAME + "." +TestUtils.TABLE_NAME);
        }
        } catch (Exception e) {
        // Ignore
        }
    }

  @Test
  public void testConfig() {
    AggregatedHttpResponse resp =
      client.get("/v1/config").aggregate().join();
    assertEquals("{\"defaults\":{},\"overrides\":{}}", resp.contentUtf8());
  }

  @Test
  public void testNamespaces()
    throws ApiException, IOException {
    CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(
      new CreateSchema()
        .catalogName(TestUtils.CATALOG_NAME)
        .name(TestUtils.SCHEMA_NAME)
    );
    // GetNamespace for catalog
    {
      AggregatedHttpResponse resp =
        client.get("/v1/namespaces/" + TestUtils.CATALOG_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      Assert.assertEquals(GetNamespaceResponse.builder()
          .withNamespace(Namespace.of(TestUtils.CATALOG_NAME))
          .build()
          .toString(),
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), GetNamespaceResponse.class)
          .toString());
    }
    // GetNamespace for schema
    {
      AggregatedHttpResponse resp =
        client.get("/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME)
          .aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      Assert.assertEquals(GetNamespaceResponse.builder()
          .withNamespace(Namespace.of(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME))
          .build()
          .toString(),
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), GetNamespaceResponse.class)
          .toString());
    }

    // ListNamespaces from root
    {
      AggregatedHttpResponse resp =
        client.get("/v1/namespaces").aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      Assert.assertEquals(ListNamespacesResponse.builder()
          .add(Namespace.of(TestUtils.CATALOG_NAME))
          .build()
          .toString(),
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListNamespacesResponse.class)
          .toString());
    }
    // ListNamespaces from catalog
    {
      AggregatedHttpResponse resp =
        client.get("/v1/namespaces?parent=" + TestUtils.CATALOG_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      Assert.assertEquals(ListNamespacesResponse.builder()
          .add(Namespace.of(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME))
          .build()
          .toString(),
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListNamespacesResponse.class)
          .toString());
    }
    // ListNamespaces from schema
    {
      AggregatedHttpResponse resp =
        client.get("/v1/namespaces?parent=" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME)
          .aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      Assert.assertEquals(ListNamespacesResponse.builder()
          .build()
          .toString(),
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListNamespacesResponse.class)
          .toString());
    }
  }

  @Test
  public void testTable() throws ApiException, IOException, URISyntaxException {
    CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(
      new CreateSchema()
        .catalogName(TestUtils.CATALOG_NAME)
        .name(TestUtils.SCHEMA_NAME)
    );
    ColumnInfo columnInfo1 = new ColumnInfo().name("as_int").typeText("INTEGER")
      .typeJson("{\"type\": \"integer\"}")
      .typeName(ColumnTypeName.INT).typePrecision(10).typeScale(0).position(0)
      .comment("Integer column").nullable(true);
    ColumnInfo columnInfo2 = new ColumnInfo().name("as_string").typeText("VARCHAR(255)")
      .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
      .typeName(ColumnTypeName.STRING).position(1)
      .comment("String column").nullable(true);
    CreateTable createTableRequest = new CreateTable()
      .name(TestUtils.TABLE_NAME)
      .catalogName(TestUtils.CATALOG_NAME)
      .schemaName(TestUtils.SCHEMA_NAME)
      .columns(List.of(columnInfo1, columnInfo2))
      .comment(TestUtils.COMMENT)
      .storageLocation("/tmp/stagingLocation")
      .tableType(TableType.EXTERNAL)
      .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableInfo = tableOperations.createTable(createTableRequest);

    // Uniform table doesn't exist at this point
    {
      AggregatedHttpResponse resp =
        client.head(
          "/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "/tables/" +
            TestUtils.TABLE_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 404);
    }
    {
      AggregatedHttpResponse resp =
        client.get(
          "/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "/tables/" +
            TestUtils.TABLE_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 404);
    }

    // Add the uniform metadata
    try (Session session = HibernateUtil.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      TableInfoDAO tableInfoDAO = TableInfoDAO.builder().build();
      assert tableInfo.getTableId() != null;
      session.load(tableInfoDAO, UUID.fromString(tableInfo.getTableId()));
      String metadataLocation = Objects.requireNonNull(
        this.getClass().getResource("/metadata.json")).toURI().toString();
      tableInfoDAO.setUniformIcebergMetadataLocation(metadataLocation);
      session.merge(tableInfoDAO);
      tx.commit();
    }

    // Now the uniform table exists
    {
      AggregatedHttpResponse resp =
        client.head(
          "/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "/tables/" +
            TestUtils.TABLE_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
    }
    // metadata is valid metadata content and metadata location matches
    {
      AggregatedHttpResponse resp =
        client.get(
          "/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "/tables/" +
            TestUtils.TABLE_NAME).aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      LoadTableResponse loadTableResponse =
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), LoadTableResponse.class);
      Assert.assertEquals(this.getClass().getResource("/metadata.json").toURI().toString(),
        loadTableResponse.tableMetadata().metadataFileLocation());
    }

    // List uniform tables
    {
      AggregatedHttpResponse resp =
        client.get(
          "/v1/namespaces/" + TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "/tables").aggregate().join();
      Assert.assertEquals(resp.status().code(), 200);
      ListTablesResponse loadTableResponse =
        RESTObjectMapper.mapper().readValue(resp.contentUtf8(), ListTablesResponse.class);
      Assert.assertEquals(List.of(TableIdentifier.of(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME)),
        loadTableResponse.identifiers());
    }
  }

  @Test
  public void testLoadTablesInvalidNamespace() {
    AggregatedHttpResponse resp = client.get("/v1/namespaces/incomplete_namespace/tables/some_table").aggregate().join();
    Assert.assertEquals(resp.status().code(), 400);
  }

  @Test
  public void testListTablesInvalidNamespace() {
    AggregatedHttpResponse resp = client.get("/v1/namespaces/incomplete_namespace/tables").aggregate().join();
    Assert.assertEquals(resp.status().code(), 400);
  }

  @Test
  public void testTableExistsInvalidNamespace() {
    AggregatedHttpResponse resp = client.head("/v1/namespaces/incomplete_namespace/tables").aggregate().join();
    Assert.assertEquals(resp.status().code(), 400);
  }
}
