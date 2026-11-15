package io.unitycatalog.cli.utils;

import io.unitycatalog.client.ApiException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
   * Lists from {@code --page_token}, or from the start when it is not given, following each
   * response's next page token until the server returns no more pages.
   *
   * @param json the command's parameters. A positive {@code --max_results} limits the total number
   *     of results across all pages. 0, like a missing value, means no limit, and a negative value
   *     is passed to the server, which rejects it.
   * @param fetchPage requests one page with the given max results and page token
   * @param getItems returns the items of a page, or null when it has none
   * @param getNextPageToken returns the token of the next page, or null or empty after the last one
   * @return the items of all the pages, in order
   * @throws ApiException if a page request fails
   * @throws CliException if the server returns a page token that was already requested
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
    Set<String> requestedTokens = new HashSet<>();
    requestedTokens.add(pageToken);
    List<T> results = new ArrayList<>();
    while (true) {
      Integer pageMaxResults = maxResults;
      if (isLimited) {
        pageMaxResults = maxResults - results.size();
      }
      R page = fetchPage.fetch(pageMaxResults, pageToken);
      List<T> items = getItems.apply(page);
      if (items != null) {
        results.addAll(items);
      }
      String nextPageToken = getNextPageToken.apply(page);
      if (nextPageToken == null
          || nextPageToken.isEmpty()
          || (isLimited && results.size() >= maxResults)) {
        return results;
      }
      if (!requestedTokens.add(nextPageToken)) {
        throw new CliException(
            "The server returned page token " + nextPageToken + " again, so the listing stopped.");
      }
      pageToken = nextPageToken;
    }
  }
}
