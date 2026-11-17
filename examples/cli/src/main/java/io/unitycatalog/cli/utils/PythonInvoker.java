package io.unitycatalog.cli.utils;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonInvoker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonInvoker.class);

  /** How much of the end of stderr a failure message keeps. The Python error is printed last. */
  private static final int MAX_ERROR_OUTPUT_BYTES = 4096;

  public static String invokePython(FunctionInfo function, String scriptPath, String... args)
      throws ApiException {
    return invokePython(
        function, scriptPath, Paths.get(System.getProperty("java.io.tmpdir")), args);
  }

  /** Same as above, with the stderr file of the call created in {@code tempDir}. */
  static String invokePython(FunctionInfo function, String scriptPath, Path tempDir, String... args)
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

      // Send stderr to a file, so that a full stderr pipe cannot block the script while stdout is
      // read.
      Path errorOutput = Files.createTempFile(tempDir, "uc-function-", ".stderr");
      try {
        return runScript(argsList, errorOutput);
      } finally {
        deleteQuietly(errorOutput);
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

  private static String runScript(List<String> command, Path errorOutput)
      throws IOException, InterruptedException, ApiException {
    Process p = new ProcessBuilder(command).redirectError(errorOutput.toFile()).start();
    try {
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
        String stderr = readEnd(errorOutput).strip();
        throw new ApiException(
            "Python function failed with exit code "
                + exitCode
                + (stderr.isEmpty() ? "" : ": " + stderr));
      }
      return result.toString();
    } finally {
      p.destroy();
    }
  }

  /** Reads the last MAX_ERROR_OUTPUT_BYTES of the file, with "..." in front if it is longer. */
  private static String readEnd(Path file) throws IOException {
    try (RandomAccessFile in = new RandomAccessFile(file.toFile(), "r")) {
      long length = in.length();
      long start = Math.max(0, length - MAX_ERROR_OUTPUT_BYTES);
      byte[] bytes = new byte[(int) (length - start)];
      in.seek(start);
      in.readFully(bytes);
      int from = 0;
      // Skip the rest of a UTF-8 character that the cut split.
      while (start > 0 && from < bytes.length && (bytes[from] & 0xC0) == 0x80) {
        from++;
      }
      String end = new String(bytes, from, bytes.length - from, StandardCharsets.UTF_8);
      return start > 0 ? "..." + end : end;
    }
  }

  /** Deletes the file, or schedules it for deletion at exit, so cleanup never hides the result. */
  static void deleteQuietly(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      LOGGER.debug("Could not delete {}, deleting it at exit instead", file);
      file.toFile().deleteOnExit();
    }
  }
}
