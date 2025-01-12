package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.RepositoryFactory;

@ExceptionHandler(GlobalExceptionHandler.class)
public class MetastoreService {
  private final MetastoreRepository metastoreRepository;

  public MetastoreService(RepositoryFactory repositoryFactory) {
    this.metastoreRepository = repositoryFactory.getRepository(MetastoreRepository.class);
  }

  @Get("/metastore_summary")
  public HttpResponse getMetastoreSummary() {
    return HttpResponse.ofJson(metastoreRepository.getMetastoreSummary());
  }
}
