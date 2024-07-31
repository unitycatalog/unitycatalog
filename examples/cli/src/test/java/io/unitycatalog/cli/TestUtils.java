package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.base.ServerConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestUtils {
  public static ObjectMapper objectMapper = new ObjectMapper();

  public static JsonNode executeCLICommand(String[] args) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    PrintStream oldOut = System.out;
    String output;
    try {
      System.setOut(printStream);
      UnityCatalogCli.main(args);
      System.out.flush();
      output = outputStream.toString();
      return parseJsonOutput(output);
    } catch (JsonProcessingException e) {
      System.out.println("Error parsing output: " + e.getMessage());
    } finally {
      System.setOut(oldOut);
    }
    return null;
  }

  private static JsonNode parseJsonOutput(String output) throws JsonProcessingException {
    List<String> jsonLines =
        Arrays.stream(output.split(System.lineSeparator()))
            .filter(line -> line.trim().startsWith("{") || line.trim().startsWith("["))
            .collect(Collectors.toList());

    String json = String.join("", jsonLines);
    return objectMapper.readTree(json);
  }

  public static String[] addServerAndAuthParams(List<String> args, ServerConfig config) {
    List<String> extendedArgs =
        Arrays.asList(
            "--server",
            config.getServerUrl(),
            "--auth_token",
            config.getAuthToken(),
            "--output",
            "json");
    List<String> allArgs = new ArrayList<>(args);
    allArgs.addAll(0, extendedArgs);
    return allArgs.toArray(new String[0]);
  }
}
