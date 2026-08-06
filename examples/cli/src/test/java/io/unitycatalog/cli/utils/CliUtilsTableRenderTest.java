package io.unitycatalog.cli.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the human-readable (non-JSON) table rendering path in {@link CliUtils}. The CLI
 * integration tests always pass {@code --output json}, so they never exercise these code paths;
 * these tests cover them directly.
 */
public class CliUtilsTableRenderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode json(String raw) {
    try {
      return MAPPER.readTree(raw);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void rowsRenderHeadersUpperCasedAndCentered() {
    JsonNode node = json("[{\"name\":\"unity\",\"comment\":\"the default catalog\"}]");

    String table = CliUtils.processOutputAsRows(node, 120);

    // Headers are upper-cased.
    assertThat(table).contains("NAME").contains("COMMENT");
    // BASIC_ASCII border draws a rule between rows.
    assertThat(table).contains("+").contains("|");
  }

  @Test
  public void rowsDoNotOverTruncateShortValues() {
    // Regression: freva treats Column.maxWidth as content + 2*padding, so feeding
    // the raw content width truncated short values (e.g. "unity" -> "un…"). The
    // full value must survive at a comfortable output width.
    JsonNode node = json("[{\"name\":\"unity\"}]");

    String table = CliUtils.processOutputAsRows(node, 120);

    assertThat(table).contains("unity");
    assertThat(table).doesNotContain("…");
  }

  @Test
  public void rowsTruncateLongValuesWithEllipsis() {
    String longValue = "a really long comment that exceeds any reasonable column width here";
    // "comment" is not a fixed-width column (unlike name/*id), so it participates
    // in width shrinking and gets truncated at a narrow output width.
    JsonNode node = json("[{\"name\":\"unity\",\"comment\":\"" + longValue + "\"}]");

    String table = CliUtils.processOutputAsRows(node, 30);

    assertThat(table).contains("…");
    assertThat(table).doesNotContain(longValue);
  }

  @Test
  public void rowsRenderWhenAllColumnsAreFixedWidthAndOverflow() {
    // Regression: name and *id columns are treated as fixed-width. When every
    // column is fixed-width and the row overflows the output width, the width
    // redistribution divisor was zero, throwing ArithmeticException. It must now
    // render (nothing to shrink) instead.
    String longName = "a_really_long_catalog_name_that_overflows_a_narrow_terminal_width";
    JsonNode node = json("[{\"name\":\"" + longName + "\",\"catalog_id\":\"1234567890\"}]");

    assertThatCode(() -> CliUtils.processOutputAsRows(node, 20)).doesNotThrowAnyException();
  }

  @Test
  public void rowsRenderAtNarrowWidthsWithoutThrowing() {
    // Regression: narrow widths previously produced a negative content length and
    // threw "Range [0, -N) out of bounds".
    JsonNode node =
        json("[{\"name\":\"unity\",\"catalog_type\":\"MANAGED_CATALOG\",\"comment\":\"hi\"}]");

    for (int width : new int[] {40, 60, 80}) {
      assertThatCode(() -> CliUtils.processOutputAsRows(node, width)).doesNotThrowAnyException();
    }
  }

  @Test
  public void keyValueRendersKeysUpperCasedAndValues() {
    JsonNode node = json("{\"name\":\"unity\",\"owner\":\"alice\"}");

    String table = CliUtils.processOutputAsKeysAndValues(node, 120);

    assertThat(table).contains("KEY").contains("VALUE");
    assertThat(table).contains("NAME").contains("unity");
    assertThat(table).contains("OWNER").contains("alice");
  }

  @Test
  public void keyValueWrapsArrayValuesOntoMultipleLines() {
    JsonNode node = json("{\"columns\":[\"col_a(int)\",\"col_b(string)\",\"col_c(long)\"]}");

    String table = CliUtils.processOutputAsKeysAndValues(node, 120);

    // Each array element is rendered; array values are joined so they wrap onto
    // separate lines rather than being truncated.
    assertThat(table).contains("col_a(int)").contains("col_b(string)").contains("col_c(long)");
  }

  @Test
  public void keyValueRendersAtNarrowWidthsWithoutThrowing() {
    JsonNode node = json("{\"name\":\"unity\",\"comment\":\"a fairly long comment value here\"}");

    for (int width : new int[] {40, 60, 80}) {
      assertThatCode(() -> CliUtils.processOutputAsKeysAndValues(node, width))
          .doesNotThrowAnyException();
    }
  }
}
