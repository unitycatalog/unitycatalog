package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.utils.Constants;
import java.util.Map;
import java.util.UUID;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the diagnostics {@link PropertyRepository} emits when a property write fails, e.g.
 * a value longer than the property_value column.
 */
public class PropertyRepositoryTest {

  @Test
  public void describeReportsKeyAndValueLength() {
    assertThat(PropertyRepository.describe(Map.of("spark.sql.a", "x".repeat(300))))
        .contains("spark.sql.a (300 chars) = " + "x".repeat(300));
  }

  @Test
  public void describeTruncatesVeryLongValues() {
    String described = PropertyRepository.describe(Map.of("k", "x".repeat(10_000)));
    assertThat(described).contains("k (10000 chars) = ");
    assertThat(described).hasSizeLessThan(9_000);
  }

  @Test
  public void persistAllFlushesAndRethrowsFailures() {
    Session session = mock(Session.class);
    RuntimeException failure = new RuntimeException("Data truncation: Data too long for column");
    doThrow(failure).when(session).flush();

    assertThatThrownBy(
            () ->
                PropertyRepository.persistAll(
                    session,
                    PropertyDAO.from(
                        Map.of("k", "x".repeat(300)), UUID.randomUUID(), Constants.TABLE)))
        .isSameAs(failure);
    verify(session).flush();
  }
}
