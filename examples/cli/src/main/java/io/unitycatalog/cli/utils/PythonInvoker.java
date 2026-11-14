package io.unitycatalog.cli.utils;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.json.JSONArray;

public class PythonInvoker {

  public static String invokePython(FunctionInfo function, String scriptPath, String... args)
      throws ApiException {
    try {
      // Prepare the arguments for the Python script
      List<String> argsList = new ArrayList<>();
      argsList.add("python3");
      argsList.add(scriptPath);

      // Add function name and routine body
      argsList.add(function.getName());
      argsList.add(function.getRoutineDefinition());

      // Retrieve and add parameters as arguments
      List<FunctionParameterInfo> parameters = function.getInputParams().getParameters();
      if (parameters == null || parameters.isEmpty()) {
        throw new ApiException("Function parameters not found.");
      }
      if (args.length < parameters.size()) {
        List<String> names =
            parameters.stream()
                .skip(args.length)
                .map(FunctionParameterInfo::getName)
                .collect(Collectors.toList());
        throw new ApiException(
            "Not enough parameters provided: "
                + args.length
                + ", expected: "
                + parameters.size()
                + ", missing: "
                + names);
      }
      List<String> paramNames = new ArrayList<>();
      List<Object> argValues = new ArrayList<>();
      for (FunctionParameterInfo param : parameters) {
        paramNames.add(param.getName());
        String argument = args[param.getPosition()];
        if (param.getTypeName().equals(ColumnTypeName.INT)) {
          argValues.add(Integer.parseInt(argument));
        } else if (param.getTypeName().equals(ColumnTypeName.DOUBLE)) {
          argValues.add(Double.parseDouble(argument));
        } else if (param.getTypeName().equals(ColumnTypeName.STRING)) {
          argValues.add(argument);
        } else {
          throw new ApiException("Unsupported parameter type: " + param.getTypeName());
        }
        // Example values, these should be dynamically provided based on use case
      }

      // Add parameter names and values
      argsList.add(String.join(", ", paramNames));
      argsList.add(new JSONArray(argValues).toString());

      // Invoke the Python script
      ProcessBuilder pb = new ProcessBuilder(argsList);
      Process p = pb.start();
      try {
        // Read stderr on another thread, so that a full stderr pipe cannot block the script while
        // stdout is read below.
        CompletableFuture<String> error =
            CompletableFuture.supplyAsync(() -> readFully(p.getErrorStream()));

        StringBuilder result = new StringBuilder();
        try (BufferedReader in =
            new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
          String resultLine;
          while ((resultLine = in.readLine()) != null) {
            result.append(resultLine).append("\n");
          }
        }
        int exitCode = p.waitFor();
        if (exitCode != 0) {
          String stderr = error.join().strip();
          throw new ApiException(
              "Python function failed with exit code "
                  + exitCode
                  + (stderr.isEmpty() ? "" : ": " + stderr));
        }
        return result.toString();
      } finally {
        p.destroy();
      }
    } catch (ApiException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApiException("Interrupted while invoking Python script");
    } catch (Exception e) {
      e.printStackTrace();
      throw new ApiException("Error invoking Python script: " + e.getMessage());
    }
  }

  private static String readFully(InputStream stream) {
    try (stream) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
