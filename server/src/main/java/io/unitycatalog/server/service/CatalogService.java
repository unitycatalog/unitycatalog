package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListCatalogsResponse;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.model.ListRegisteredModelsResponse;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.ListVolumesResponseContent;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService extends AuthorizedService {
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final SchemaRepository schemaRepository;
  private final TableRepository tableRepository;
  private final VolumeRepository volumeRepository;
  private final FunctionRepository functionRepository;
  private final ModelRepository modelRepository;

  @SneakyThrows
  public CatalogService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.tableRepository = repositories.getTableRepository();
    this.volumeRepository = repositories.getVolumeRepository();
    this.functionRepository = repositories.getFunctionRepository();
    this.modelRepository = repositories.getModelRepository();
  }

  /**
   * Creating a catalog requires one of OWNER or CREATE_CATALOG permission on metastore.
   * Additionally, if a {@code storage_root} is specified:
   *
   * <ul>
   *   <li>The path has to map to an external_location, not anything else (tables, volumes, models),
   *       to make sure the path isn't under any existing data securables.
   *   <li>User needs to have OWNER or CREATE_MANAGED_STORAGE permission on the external location.
   * </ul>
   *
   * {@code storage_root} is annotated as both a {@link AuthorizeResourceKey} (which maps to owning
   * resource) and {@link AuthorizeKey} (which is simply the raw value of storage_root). This is
   * done so that the expression can check both: 1. if parameter is specified, and 2. which external
   * location (rather than tables etc.) owns the path.
   */
  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #metastore, OWNER, CREATE_CATALOG) &&
      (#storage_root == null ||
       (#no_overlap_with_data_securable &&
        #external_location != null &&
        #authorizeAny(#principal, #external_location, OWNER, CREATE_MANAGED_STORAGE)))
    """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createCatalog(
      @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_root")
          @AuthorizeKey(key = "storage_root")
          CreateCatalog createCatalog) {
    CatalogInfo catalogInfo = catalogRepository.addCatalog(createCatalog);
    initializeBasicAuthorization(catalogInfo.getId());
    return HttpResponse.ofJson(catalogInfo);
  }

  private static final String LIST_AND_GET_AUTH_EXPRESSION = """
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """;

  @Get("")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listCatalogs(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListCatalogsResponse listCatalogsResponse =
        catalogRepository.listCatalogs(maxResults, pageToken);
    applyResponseFilter(SecurableType.CATALOG, listCatalogsResponse.getCatalogs());
    return HttpResponse.ofJson(listCatalogsResponse);
  }

  @Get("/{name}")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getCatalog(@Param("name") @AuthorizeResourceKey(CATALOG) String name) {
    return HttpResponse.ofJson(catalogRepository.getCatalog(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateCatalog(
      @Param("name") @AuthorizeResourceKey(CATALOG) String name,
      UpdateCatalog updateCatalog) {
    return HttpResponse.ofJson(catalogRepository.updateCatalog(name, updateCatalog));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteCatalog(
      @Param("name") @AuthorizeResourceKey(CATALOG) String name,
      @Param("force") Optional<Boolean> force) {
    CatalogInfo catalogInfo = catalogRepository.getCatalog(name);
    boolean isForceDelete = force.orElse(false);
    List<SchemaAuthorizationSubtree> schemaSubtrees =
        isForceDelete ? collectSchemaAuthorizationSubtrees(catalogInfo) : List.of();

    catalogRepository.deleteCatalog(name, isForceDelete);

    if (isForceDelete) {
      clearForceDeletedCatalogAuthorizations(catalogInfo.getId(), schemaSubtrees);
    }
    removeAuthorizations(catalogInfo.getId());
    return HttpResponse.of(HttpStatus.OK);
  }

  private List<SchemaAuthorizationSubtree> collectSchemaAuthorizationSubtrees(
      CatalogInfo catalogInfo) {
    List<SchemaAuthorizationSubtree> schemaSubtrees = new ArrayList<>();
    String catalogName = catalogInfo.getName();
    String nextSchemaPageToken = null;
    do {
      ListSchemasResponse schemaPage =
          schemaRepository.listSchemas(
              catalogName,
              Optional.empty(),
              Optional.ofNullable(nextSchemaPageToken));
      for (SchemaInfo schema : schemaPage.getSchemas()) {
        schemaSubtrees.add(
            new SchemaAuthorizationSubtree(
                schema.getSchemaId(),
                listChildResourceIds(catalogName, schema.getName())));
      }
      nextSchemaPageToken = schemaPage.getNextPageToken();
    } while (nextSchemaPageToken != null);
    return schemaSubtrees;
  }

  private List<String> listChildResourceIds(String catalogName, String schemaName) {
    List<String> childResourceIds = new ArrayList<>();
    childResourceIds.addAll(listAllTableIds(catalogName, schemaName));
    childResourceIds.addAll(listAllVolumeIds(catalogName, schemaName));
    childResourceIds.addAll(listAllFunctionIds(catalogName, schemaName));
    childResourceIds.addAll(listAllRegisteredModelIds(catalogName, schemaName));
    return childResourceIds;
  }

  private List<String> listAllTableIds(String catalogName, String schemaName) {
    List<String> tableIds = new ArrayList<>();
    String nextPageToken = null;
    do {
      ListTablesResponse response =
          tableRepository.listTables(
              catalogName,
              schemaName,
              Optional.empty(),
              Optional.ofNullable(nextPageToken),
              true,
              true);
      for (TableInfo table : response.getTables()) {
        tableIds.add(table.getTableId());
      }
      nextPageToken = response.getNextPageToken();
    } while (nextPageToken != null);
    return tableIds;
  }

  private List<String> listAllVolumeIds(String catalogName, String schemaName) {
    List<String> volumeIds = new ArrayList<>();
    String nextPageToken = null;
    do {
      ListVolumesResponseContent response =
          volumeRepository.listVolumes(
              catalogName,
              schemaName,
              Optional.empty(),
              Optional.ofNullable(nextPageToken),
              Optional.empty());
      for (VolumeInfo volume : response.getVolumes()) {
        volumeIds.add(volume.getVolumeId());
      }
      nextPageToken = response.getNextPageToken();
    } while (nextPageToken != null);
    return volumeIds;
  }

  private List<String> listAllFunctionIds(String catalogName, String schemaName) {
    List<String> functionIds = new ArrayList<>();
    String nextPageToken = null;
    do {
      ListFunctionsResponse response =
          functionRepository.listFunctions(
              catalogName, schemaName, Optional.empty(), Optional.ofNullable(nextPageToken));
      for (FunctionInfo function : response.getFunctions()) {
        functionIds.add(function.getFunctionId());
      }
      nextPageToken = response.getNextPageToken();
    } while (nextPageToken != null);
    return functionIds;
  }

  private List<String> listAllRegisteredModelIds(String catalogName, String schemaName) {
    List<String> modelIds = new ArrayList<>();
    String nextPageToken = null;
    do {
      ListRegisteredModelsResponse response =
          modelRepository.listRegisteredModels(
              Optional.of(catalogName),
              Optional.of(schemaName),
              Optional.empty(),
              Optional.ofNullable(nextPageToken));
      for (RegisteredModelInfo model : response.getRegisteredModels()) {
        modelIds.add(model.getId());
      }
      nextPageToken = response.getNextPageToken();
    } while (nextPageToken != null);
    return modelIds;
  }

  private void clearForceDeletedCatalogAuthorizations(
      String catalogId, List<SchemaAuthorizationSubtree> schemaSubtrees) {
    for (SchemaAuthorizationSubtree schemaSubtree : schemaSubtrees) {
      removeSchemaAuthorizationSubtree(
          schemaSubtree.schemaId(), catalogId, schemaSubtree.childResourceIds());
    }
  }

  private record SchemaAuthorizationSubtree(String schemaId, List<String> childResourceIds) {}
}
