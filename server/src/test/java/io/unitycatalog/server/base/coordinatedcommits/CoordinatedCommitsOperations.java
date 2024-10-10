package io.unitycatalog.server.base.coordinatedcommits;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.Commit;
import io.unitycatalog.client.model.GetCommits;
import io.unitycatalog.client.model.GetCommitsResponse;

public interface CoordinatedCommitsOperations {
  void commit(Commit commit) throws ApiException;

  GetCommitsResponse getCommits(GetCommits getCommits) throws ApiException;
}
