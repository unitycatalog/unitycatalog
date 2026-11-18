package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.base.ServerConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TestUtils {
  public static ObjectMapper objectMapper = new ObjectMapper();

  public static JsonNode executeCliCommand(List<String> args) {
    String output = executeCliCommandForOutput(args);
    try {
      return parseJsonOutput(output);
    } catch (JsonProcessingException e) {
      System.out.println("Error parsing output: " + e.getMessage());
    }
    return null;
  }

  public static JsonNode executeCliCommand(ServerConfig serverConfig, List<String> args) {
    List<String> commandLine = new ArrayList<>(args);
    addServerAndAuthParams(commandLine, serverConfig);
    return executeCliCommand(commandLine);
  }

  /** Runs a CLI command and returns what it printed, for commands whose output is not JSON. */
  public static String executeCliCommandForOutput(ServerConfig serverConfig, List<String> args) {
    List<String> commandLine = new ArrayList<>(args);
    addServerAndAuthParams(commandLine, serverConfig);
    return executeCliCommandForOutput(commandLine);
  }

  private static String executeCliCommandForOutput(List<String> args) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream oldOut = System.out;
    try {
      System.setOut(new PrintStream(outputStream));
      UnityCatalogCli.main(args.toArray(String[]::new));
      System.out.flush();
      return outputStream.toString();
    } finally {
      System.setOut(oldOut);
    }
  }

  private static JsonNode parseJsonOutput(String output) throws JsonProcessingException {
    List<String> jsonLines =
        Arrays.stream(output.split(System.lineSeparator()))
            .filter(line -> line.trim().startsWith("{") || line.trim().startsWith("["))
            .collect(Collectors.toList());

    String json = String.join("", jsonLines);
    return objectMapper.readTree(json);
  }

  public static void addServerAndAuthParams(List<String> args, ServerConfig config) {
    Collections.addAll(
        args,
        "--server",
        config.getServerUrl(),
        "--auth_token",
        config.getAuthToken(),
        "--output",
        "json");
  }
}
