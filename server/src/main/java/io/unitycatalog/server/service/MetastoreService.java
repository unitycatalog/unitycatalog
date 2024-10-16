package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.persist.MetastoreRepository;

@ExceptionHandler(GlobalExceptionHandler.class)
public class MetastoreService {
  private static final MetastoreRepository METASTORE_REPOSITORY = MetastoreRepository.getInstance();

  @Get("/metastore_summary")
  public HttpResponse getMetastoreSummary() {
    return HttpResponse.ofJson(METASTORE_REPOSITORY.getMetastoreSummary());
  }
}
