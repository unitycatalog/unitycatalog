package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.UnityCatalogServer;
import java.util.List;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import org.junit.jupiter.api.Test;

/**
 * hadoop-client-runtime registers a relocated Woodstox that Jackson XML (S3Mock, the Azure SDK)
 * can't use, so StAX must resolve to Woodstox's own factories.
 */
public class StaxProviderTest {

  private static final List<String> PROPERTIES =
      List.of(
          "javax.xml.stream.XMLInputFactory",
          "javax.xml.stream.XMLOutputFactory",
          "javax.xml.stream.XMLEventFactory");

  @Test
  public void staxResolvesToWoodstox() {
    assertThat(XMLInputFactory.newInstance().getClass().getName())
        .isEqualTo("com.ctc.wstx.stax.WstxInputFactory");
    assertThat(XMLOutputFactory.newInstance().getClass().getName())
        .isEqualTo("com.ctc.wstx.stax.WstxOutputFactory");
    assertThat(XMLEventFactory.newInstance().getClass().getName())
        .isEqualTo("com.ctc.wstx.stax.WstxEventFactory");
  }

  @Test
  public void serverSetsWoodstoxUnlessTheOperatorChoseAnother() {
    List<String> saved = PROPERTIES.stream().map(System::getProperty).toList();
    try {
      PROPERTIES.forEach(System::clearProperty);
      System.setProperty(PROPERTIES.get(0), "custom.InputFactory");
      UnityCatalogServer.useWoodstoxForStax();
      assertThat(System.getProperty(PROPERTIES.get(0))).isEqualTo("custom.InputFactory");
      assertThat(System.getProperty(PROPERTIES.get(1)))
          .isEqualTo("com.ctc.wstx.stax.WstxOutputFactory");
      assertThat(System.getProperty(PROPERTIES.get(2)))
          .isEqualTo("com.ctc.wstx.stax.WstxEventFactory");
    } finally {
      for (int i = 0; i < PROPERTIES.size(); i++) {
        if (saved.get(i) == null) {
          System.clearProperty(PROPERTIES.get(i));
        } else {
          System.setProperty(PROPERTIES.get(i), saved.get(i));
        }
      }
    }
  }
}
