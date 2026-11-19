package io.unitycatalog.cli.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class PaginationTest {

  private record Page(List<String> items, String nextPageToken) {}

  private record Request(Integer maxResults, String pageToken) {}

  /** Serves the given pages in order and records each request. */
  private static final class FakeServer implements Pagination.PageFetcher<Page> {
    private final List<Page> pages;
    private final List<Request> requests = new ArrayList<>();

    FakeServer(Page... pages) {
      this.pages = Arrays.asList(pages);
    }

    @Override
    public Page fetch(Integer maxResults, String pageToken) {
      requests.add(new Request(maxResults, pageToken));
      return pages.get(requests.size() - 1);
    }

    List<String> list(JSONObject json) throws ApiException {
      return Pagination.listAllPages(json, this, Page::items, Page::nextPageToken);
    }
  }

  @Test
  public void followsEveryPageWithoutALimit() throws ApiException {
    FakeServer server =
        new FakeServer(new Page(List.of("a", "b"), "t1"), new Page(List.of("c"), null));

    assertThat(server.list(new JSONObject())).containsExactly("a", "b", "c");
    assertThat(server.requests).containsExactly(new Request(null, null), new Request(null, "t1"));
  }

  @Test
  public void stopsAtTheLimitWithoutAnExtraFetch() throws ApiException {
    FakeServer server =
        new FakeServer(
            new Page(List.of("a", "b"), "t1"),
            new Page(List.of("c"), "t2"),
            new Page(List.of("d"), null));

    assertThat(server.list(new JSONObject().put("max_results", 3))).containsExactly("a", "b", "c");
    assertThat(server.requests).containsExactly(new Request(3, null), new Request(1, "t1"));
  }

  @Test
  public void zeroMaxResultsMeansNoLimit() throws ApiException {
    FakeServer server = new FakeServer(new Page(List.of("a"), "t1"), new Page(List.of("b"), null));

    assertThat(server.list(new JSONObject().put("max_results", 0))).containsExactly("a", "b");
    assertThat(server.requests).containsExactly(new Request(0, null), new Request(0, "t1"));
  }

  @Test
  public void passesANegativeMaxResultsToTheServer() throws ApiException {
    FakeServer server = new FakeServer(new Page(List.of(), null));

    server.list(new JSONObject().put("max_results", -1));

    assertThat(server.requests).containsExactly(new Request(-1, null));
  }

  @Test
  public void startsAtTheGivenPageToken() throws ApiException {
    FakeServer server = new FakeServer(new Page(List.of("c"), null));

    assertThat(server.list(new JSONObject().put("page_token", "t1"))).containsExactly("c");
    assertThat(server.requests).containsExactly(new Request(null, "t1"));
  }

  @Test
  public void stopsAtAnEmptyNextPageToken() throws ApiException {
    FakeServer server = new FakeServer(new Page(List.of("a"), ""), new Page(List.of("b"), null));

    assertThat(server.list(new JSONObject())).containsExactly("a");
    assertThat(server.requests).hasSize(1);
  }

  @Test
  public void treatsANullItemListAsEmpty() throws ApiException {
    FakeServer server = new FakeServer(new Page(null, "t1"), new Page(List.of("a"), null));

    assertThat(server.list(new JSONObject())).containsExactly("a");
  }

  @Test
  public void failsWhenTheServerRepeatsAPageToken() {
    FakeServer server = new FakeServer(new Page(List.of("a"), "t1"), new Page(List.of("b"), "t1"));

    assertThatThrownBy(() -> server.list(new JSONObject()))
        .isInstanceOf(CliException.class)
        .hasMessageContaining("t1");
    assertThat(server.requests).hasSize(2);
  }

  @Test
  public void failsWhenTheServerReturnsTheStartingPageToken() {
    FakeServer server = new FakeServer(new Page(List.of("a"), "t0"));

    assertThatThrownBy(() -> server.list(new JSONObject().put("page_token", "t0")))
        .isInstanceOf(CliException.class);
    assertThat(server.requests).hasSize(1);
  }
}
