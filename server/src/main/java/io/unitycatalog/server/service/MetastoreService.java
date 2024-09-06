package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.persist.MetastoreRepository;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class MetastoreService {
  private static final MetastoreRepository METASTORE_REPOSITORY = MetastoreRepository.getInstance();

  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public MetastoreService(UnityCatalogAuthorizer authorizer) {
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  // TODO: should this endpoint be open to all?
  @Get("/metastore_summary")
  public HttpResponse getMetastoreSummary() {
    return HttpResponse.ofJson(METASTORE_REPOSITORY.getMetastoreSummary());
  }
}
