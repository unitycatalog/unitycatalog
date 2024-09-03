package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.ModelRepository;
import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ModelService {

  private static final ModelRepository MODEL_REPOSITORY = ModelRepository.getInstance();

  public ModelService() {}

  @Post("")
  public HttpResponse createRegisteredModel(CreateRegisteredModel createRegisteredModel) {
    assert createRegisteredModel != null;
    RegisteredModelInfo createRegisteredModelResponse =
        MODEL_REPOSITORY.createRegisteredModel(createRegisteredModel);
    return HttpResponse.ofJson(createRegisteredModelResponse);
  }

  @Get("")
  public HttpResponse listRegisteredModels(
      @Param("catalog_name") Optional<String> catalogName,
      @Param("schema_name") Optional<String> schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(
        MODEL_REPOSITORY.listRegisteredModels(catalogName, schemaName, maxResults, pageToken));
  }

  @Get("/{full_name}")
  public HttpResponse getRegisteredModel(@Param("full_name") String fullNameArg) {
    assert fullNameArg != null;
    RegisteredModelInfo registeredModelInfo = MODEL_REPOSITORY.getRegisteredModel(fullNameArg);
    return HttpResponse.ofJson(registeredModelInfo);
  }

  @Patch("/{full_name}")
  public HttpResponse updateRegisteredModel(UpdateRegisteredModel updateRegisteredModel) {
    assert updateRegisteredModel != null;
    RegisteredModelInfo updateRegisteredModelResponse =
        MODEL_REPOSITORY.updateRegisteredModel(updateRegisteredModel);
    return HttpResponse.ofJson(updateRegisteredModelResponse);
  }

  @Delete("/{full_name}")
  public HttpResponse deleteRegisteredModel(
      @Param("full_name") String fullName, @Param("force") Optional<Boolean> force) {
    MODEL_REPOSITORY.deleteRegisteredModel(fullName, force.orElse(false));
    return HttpResponse.of(HttpStatus.OK);
  }

  @Post("/versions")
  public HttpResponse createModelVersion(CreateModelVersion createModelVersion) {
    assert createModelVersion != null;
    ModelVersionInfo createModelVersionResponse =
        MODEL_REPOSITORY.createModelVersion(createModelVersion);
    return HttpResponse.ofJson(createModelVersionResponse);
  }

  @Get("/{full_name}/versions")
  public HttpResponse listModelVersions(
      @Param("full_name") String fullName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(MODEL_REPOSITORY.listModelVersions(fullName, maxResults, pageToken));
  }

  @Get("/{full_name}/versions/{version}")
  public HttpResponse getModelVersion(
      @Param("full_name") String fullName, @Param("version") Long version) {
    assert fullName != null && version != null;
    ModelVersionInfo modelVersionInfo = MODEL_REPOSITORY.getModelVersion(fullName, version);
    return HttpResponse.ofJson(modelVersionInfo);
  }

  @Patch("/{full_name}/versions/{version}")
  public HttpResponse updateModelVersion(UpdateModelVersion updateModelVersion) {
    assert updateModelVersion != null;
    ModelVersionInfo updateModelVersionResponse =
        MODEL_REPOSITORY.updateModelVersion(updateModelVersion);
    return HttpResponse.ofJson(updateModelVersionResponse);
  }

  @Delete("/{full_name}/versions/{version}")
  public HttpResponse deleteModelVersion(
      @Param("full_name") String fullName, @Param("version") Long version) {
    MODEL_REPOSITORY.deleteModelVersion(fullName, version);
    return HttpResponse.of(HttpStatus.OK);
  }

  @Patch("/{full_name}/versions/{version}/finalize")
  public HttpResponse finalizeModelVersion(FinalizeModelVersion finalizeModelVersion) {
    assert finalizeModelVersion != null;
    ModelVersionInfo finalizeModelVersionResponse =
        MODEL_REPOSITORY.finalizeModelVersion(finalizeModelVersion);
    return HttpResponse.ofJson(finalizeModelVersionResponse);
  }
}
