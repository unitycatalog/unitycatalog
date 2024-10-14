package io.unitycatalog.server.utils;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.cli.*;

@Setter
@Getter
public class OptionParser {
  private int port = 8080;

  private final Options options = new Options();

  public OptionParser() {
    options.addOption(
        Option.builder("p")
            .longOpt("port")
            .hasArg()
            .desc("Port number to run the server on. Default is 8080.")
            .type(Integer.class)
            .build());
    options.addOption(
        Option.builder("v")
            .longOpt("version")
            .hasArg(false)
            .desc("Display the version of the Unity Catalog server")
            .build());
    options.addOption("h", "help", false, "Print help message.");
  }

  private void printHelpAndExit(int code) {
    new HelpFormatter().printHelp("bin/start-uc-server", options);
    exit(code);
  }

  @VisibleForTesting
  protected void exit(int code) {
    System.exit(code);
  }

  /**
   * Parse the command line arguments
   *
   * @param args the command line arguments
   */
  public void parse(String[] args) {
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("h")) {
        printHelpAndExit(0);
      }
      if (cmd.hasOption("v")) {
        System.out.println(VersionUtils.VERSION);
        exit(0);
      }
      if (cmd.hasOption("p")) {
        setPort(cmd.getParsedOptionValue("p"));
      }
    } catch (ParseException e) {
      System.out.println();
      System.out.println("Parsing Failed. Reason: " + e.getMessage());
      System.out.println();
      printHelpAndExit(-1);
    }
  }
}
