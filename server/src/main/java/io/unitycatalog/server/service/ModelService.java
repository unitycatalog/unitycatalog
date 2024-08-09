package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateRegisteredModel;
import io.unitycatalog.server.model.UpdateRegisteredModel;
import io.unitycatalog.server.model.RegisteredModelInfo;
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
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(
        MODEL_REPOSITORY.listRegisteredModels(catalogName, schemaName, maxResults, pageToken));
  }

  @Get("/{full_name_arg}")
  public HttpResponse getRegisteredModel(@Param("full_name_arg") String fullNameArg) {
    assert fullNameArg != null;
    RegisteredModelInfo registeredModelInfo = MODEL_REPOSITORY.getRegisteredModel(fullNameArg);
    return HttpResponse.ofJson(registeredModelInfo);
  }

  @Patch("/{full_name_arg")
  public HttpResponse updateRegisteredModel(UpdateRegisteredModel updateRegisteredModel) {
    assert updateRegisteredModel != null;
    RegisteredModelInfo updateRegisteredModelResponse = MODEL_REPOSITORY.updateRegisteredModel(updateRegisteredModel);
    return HttpResponse.ofJson(updateRegisteredModelResponse);
  }

  @Delete("/{full_name_arg}")
  public HttpResponse deleteRegisteredModel(@Param("full_name_arg") String fullNameArg) {
    MODEL_REPOSITORY.deleteRegisteredModel(fullNameArg);
    return HttpResponse.of(HttpStatus.OK);
  }
}
