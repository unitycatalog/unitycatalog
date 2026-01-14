package io.unitycatalog.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.server.base.ServerConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Base class for CLI operation implementations that provides common functionality for executing CLI
 * commands and handling their responses.
 */
public class BaseCliOperations {
  private final String command;
  private final ServerConfig config;
  protected final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Constructs a new BaseCliOperations instance.
   *
   * @param command the CLI command name (e.g., "catalog", "schema", "table")
   * @param config the server configuration containing connection details and authentication info
   */
  public BaseCliOperations(String command, ServerConfig config) {
    this.command = command;
    this.config = config;
  }

  /**
   * Executes a CLI command and deserializes the response to the specified class type.
   *
   * <p>This method constructs and executes a CLI command, then converts the JSON response to an
   * instance of the specified class. If the class is {@code void.class} or {@code Void.class}, no
   * deserialization is performed and {@code null} is returned.
   *
   * @param <T> the type to deserialize the response into
   * @param clazz the class object representing the type T
   * @param subCommand the subcommand to execute (e.g., "get", "list", "create")
   * @param args the command-line arguments for the subcommand
   * @return the deserialized response object, or null if clazz is void/Void
   * @throws ApiException if the CLI command execution fails
   */
  public <T> T execute(Class<T> clazz, String subCommand, List<String> args) throws ApiException {
    JsonNode responseJson = executeImpl(subCommand, args);
    if (clazz == void.class || clazz == Void.class) {
      return null;
    } else {
      return objectMapper.convertValue(responseJson, clazz);
    }
  }

  /**
   * Executes a CLI command and deserializes the response using a TypeReference.
   *
   * <p>This method is useful for deserializing generic types like {@code List<CatalogInfo>} where
   * type information is needed at runtime.
   *
   * @param <T> the type to deserialize the response into
   * @param typeRef the TypeReference representing the type T
   * @param subCommand the subcommand to execute (e.g., "get", "list", "create")
   * @param args the command-line arguments for the subcommand
   * @return the deserialized response object
   * @throws ApiException if the CLI command execution fails
   */
  public <T> T execute(TypeReference<T> typeRef, String subCommand, List<String> args)
      throws ApiException {
    JsonNode responseJson = executeImpl(subCommand, args);
    return objectMapper.convertValue(responseJson, typeRef);
  }

  private JsonNode executeImpl(String subCommand, List<String> args) throws ApiException {
    try {
      List<String> commandLine = new ArrayList<>();
      commandLine.add(command);
      commandLine.add(subCommand);
      TestUtils.addServerAndAuthParams(commandLine, config);
      commandLine.addAll(args);
      return TestUtils.executeCliCommand(commandLine);
    } catch (RuntimeException e) {
      // Unwrap the ApiException which is the one we care about
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Executes an update command with separate identity and update arguments.
   *
   * <p>This method simplifies update operations by:
   *
   * <ul>
   *   <li>Automatically combining identity arguments (e.g., --name, --full_name) with update
   *       arguments
   *   <li>Handling empty update validation - the CLI rejects updates with no changes, so this
   *       method catches {@link CliException} and returns null when updateArgs is empty
   * </ul>
   *
   * @param <T> the type to deserialize the response into
   * @param clazz the class object representing the type T
   * @param identityArgs arguments that identify the resource to update (e.g., --name, --full_name)
   * @param updateArgs arguments specifying what to update (e.g., --new_name, --comment)
   * @return the updated resource, or null if updateArgs is empty and CliException was thrown
   * @throws ApiException if the CLI command execution fails for reasons other than empty update
   * @throws CliException if updateArgs is not empty but the command still fails
   */
  public <T> T executeUpdate(Class<T> clazz, List<String> identityArgs, List<String> updateArgs)
      throws ApiException {
    List<String> args = Stream.concat(identityArgs.stream(), updateArgs.stream()).toList();
    try {
      return execute(clazz, "update", args);
    } catch (CliException e) {
      if (updateArgs.isEmpty()) {
        // CLI does not allow empty update. If it throws CliException for this reason, tests want
        // to continue.
        return null;
      }
      throw e;
    }
  }
}
