package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  }

  @Test
  void testParseCLIOptionsWithVersion() {
    NoExitOptionsParser optionParser = new NoExitOptionsParser();
    optionParser.parse(new String[] {"-v"});
    assertThat(optionParser.getExitCode()).isEqualTo(0);
    assertThat(out.toString().trim()).isEqualTo(VersionUtils.VERSION);
  }

  private void verifyHelpMessage() {
    assertThat(out.toString()).contains("bin/start-uc-server");
    assertThat(out.toString())
        .contains("-p,--port <arg>   Port number to run the server on. Default is 8080.");
    assertThat(out.toString())
        .contains("-v,--version      Display the version of the Unity Catalog server");
    assertThat(out.toString()).contains("-h,--help         Print help message.");
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
