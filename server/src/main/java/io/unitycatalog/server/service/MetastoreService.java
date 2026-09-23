package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Get;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;

public class MetastoreService implements UnityCatalogRestService {
  private final MetastoreRepository metastoreRepository;

  public MetastoreService(Repositories repositories) {
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Get("/metastore_summary")
  @AuthorizeExpression("#principal != null")
  public HttpResponse getMetastoreSummary() {
    return HttpResponse.ofJson(metastoreRepository.getMetastoreSummary());
  }
}
