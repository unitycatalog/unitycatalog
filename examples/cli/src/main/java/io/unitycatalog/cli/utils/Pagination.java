package io.unitycatalog.cli.utils;

import io.unitycatalog.client.ApiException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.json.JSONObject;

/** Collects the results of a list command across all the pages the server returns. */
public final class Pagination {

  private Pagination() {}

  /** Fetches one page of a list response. */
  @FunctionalInterface
  public interface PageFetcher<R> {
    R fetch(Integer maxResults, String pageToken) throws ApiException;
  }

  /**
   * Lists from {@code --page_token} (or from the start when it is not given), following each
   * response's next page token until the server returns no more pages. A positive {@code
   * --max_results} limits the total number of results across all pages; 0, like a missing value,
   * means no limit, and a negative value is passed to the server, which rejects it.
   */
  public static <R, T> List<T> listAllPages(
      JSONObject json,
      PageFetcher<R> fetchPage,
      Function<R, List<T>> getItems,
      Function<R, String> getNextPageToken)
      throws ApiException {
    String maxResultsParam = CliParams.MAX_RESULTS.getServerParam();
    Integer maxResults = json.has(maxResultsParam) ? json.getInt(maxResultsParam) : null;
    boolean isLimited = maxResults != null && maxResults > 0;
    String pageToken = json.optString(CliParams.PAGE_TOKEN.getServerParam(), null);
    List<T> results = new ArrayList<>();
    do {
      Integer pageMaxResults =
          isLimited ? Integer.valueOf(maxResults - results.size()) : maxResults;
      R page = fetchPage.fetch(pageMaxResults, pageToken);
      List<T> items = getItems.apply(page);
      if (items != null) {
        results.addAll(items);
      }
      pageToken = getNextPageToken.apply(page);
    } while (pageToken != null
        && !pageToken.isEmpty()
        && (!isLimited || results.size() < maxResults));
    return results;
  }
}
