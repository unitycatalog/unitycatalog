package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.model.SecurableType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class UnityAccessDecoratorTest {

  @Test
  public void singleResourcesAreEvaluatedOnce() {
    Map<SecurableType, Object> keys = Map.of(METASTORE, "metastore", CATALOG, "main");
    assertThat(UnityAccessDecorator.eachResourceKeys(keys)).containsExactly(keys);
  }

  @Test
  public void severalResourcesOfOneTypeAreEvaluatedOnceEach() {
    assertThat(
            UnityAccessDecorator.eachResourceKeys(
                Map.of(CATALOG, "main", SCHEMA, List.of("main.sales", "main.hr"))))
        .containsExactly(
            Map.of(CATALOG, "main", SCHEMA, "main.sales"),
            Map.of(CATALOG, "main", SCHEMA, "main.hr"));
  }

  @Test
  public void noResourcesOfATypeAreEvaluatedOnceWithoutIt() {
    assertThat(UnityAccessDecorator.eachResourceKeys(Map.of(CATALOG, "main", SCHEMA, List.of())))
        .containsExactly(Map.of(CATALOG, "main"));
  }

  @Test
  public void severalResourcesOfTwoTypesAreRefused() {
    assertThatThrownBy(
            () ->
                UnityAccessDecorator.eachResourceKeys(
                    Map.of(CATALOG, List.of("main"), SCHEMA, List.of("main.sales"))))
        .isInstanceOf(IllegalStateException.class);
  }
}
