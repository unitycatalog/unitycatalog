package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OptionParserTest {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();

  @Setter
  @Getter
  class NoExitOptionsParser extends OptionParser {
    int exitCode = -128;

    @Override
    protected void exit(int code) {
      setExitCode(code);
    }
  }

  @BeforeEach
  void setUp() {
    System.setOut(new PrintStream(out));
  }

  @AfterEach
  void tearDown() {
    System.setOut(System.out);
  }

  @Test
  void testParseCLIOptions() {
    OptionParser optionParser = new OptionParser();
    optionParser.parse(new String[] {"-p", "8081"});
    assertThat(optionParser.getPort()).isEqualTo(8081);
    assertThat(optionParser.getObservabilityPort()).isNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8090, 9464, 65535})
  void testParseObservabilityPort(int observabilityPort) {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(
        new String[] {"--port", "9000", "--obs-port", String.valueOf(observabilityPort)});
    assertThat(optionParser.getExitCode()).isEqualTo(-128);
    assertThat(optionParser.getPort()).isEqualTo(9000);
    assertThat(optionParser.getObservabilityPort()).isEqualTo(observabilityPort);
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "2147483648"})
  void testParseInvalidObservabilityPort(String observabilityPort) {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"--obs-port", observabilityPort});
    assertThat(optionParser.getExitCode()).isEqualTo(-1);
    assertThat(out.toString()).contains("Parsing Failed");
  }

  @Test
  void testObservabilityPortRequiresValue() {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"--obs-port"});
    assertThat(optionParser.getExitCode()).isEqualTo(-1);
    assertThat(out.toString()).contains("Missing argument for option: obs-port");
  }

  @Test
  void testParseCLIOptionsWithVersion() {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"-v"});
    assertThat(optionParser.getExitCode()).isEqualTo(0);
    assertThat(out.toString().trim()).isEqualTo(VersionUtils.VERSION);
  }

  private void verifyHelpMessage() {
    String help = out.toString().replaceAll("\\s+", " ");
    assertThat(help).contains("bin/start-uc-server");
    assertThat(help).contains("-p,--port <arg> Port number to run the server on. Default is 8080.");
    assertThat(help).contains("-v,--version Display the version of the Unity Catalog server");
    assertThat(help).contains("-h,--help Print help message.");
    assertThat(help)
        .contains(
            "--obs-port <arg> Enable health and metrics on this port"
                + " (1-65535). Disabled when omitted.");
  }

  @Test
  void testParseCLIOptionsWithHelp() {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"-h"});
    assertThat(optionParser.getExitCode()).isEqualTo(0);
    verifyHelpMessage();
  }

  @Test
  void testParseCLIOptionsWithInvalidOption() {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"-x"});
    assertThat(optionParser.getExitCode()).isEqualTo(-1);
    assertThat(out.toString()).contains("Parsing Failed. Reason: Unrecognized option: -x");
    verifyHelpMessage();
  }
}
