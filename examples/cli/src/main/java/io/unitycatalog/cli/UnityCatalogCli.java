package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.commonOptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.core.config.Configurator;

public class UnityCatalogCli {

  static {
    System.setProperty("log4j.configurationFile", "etc/conf/cli.log4j2.properties");
    Configurator.initialize(null, "etc/conf/cli.log4j2.properties");
  }

  public static void main(String[] args) {
    // Create Options object
    Options options = new Options();
    Arrays.stream(CliParams.values())
        .forEach(
            cliParam ->
                options.addOption(
                    Option.builder()
                        .longOpt(cliParam.val())
                        .optionalArg(cliParam.val().equals("version"))
                        .hasArg() // See
                        // https://github.com/unitycatalog/unitycatalog/pull/398#issuecomment-2325039123
                        .build()));
    options.addOption("h", "help", false, "Print help message.");
    options.addOption("v", false, "Display the version of Unity Catalog CLI");

    // Add server specific options
    options.addOption(
        Option.builder()
            .longOpt(CliUtils.SERVER)
            .hasArg()
            .desc("UC Server to connect to. Default is reference server.")
            .build());
    options.addOption(
        Option.builder()
            .longOpt(CliUtils.AUTH_TOKEN)
            .hasArg()
            .desc("PAT token to authorize uc requests")
            .build());
    options.addOption(
        Option.builder()
            .longOpt(CliUtils.OUTPUT)
            .hasArg()
            .desc("To indicate CLI output format preference")
            .build());
    CommandLineParser parser = new DefaultParser();
    try {
      // Parse the command line arguments
      CommandLine cmd = parser.parse(options, args);

      // Check if help option is provided
      if (cmd.hasOption("h")) {
        if (cmd.getArgs().length > 1) {
          CliUtils.printSubCommandHelp(cmd.getArgs()[0], cmd.getArgs()[1]);
        } else if (cmd.getArgs().length > 0) {
          CliUtils.printEntityHelp(cmd.getArgs()[0]);
        } else {
          CliUtils.printHelp();
        }
        return;
      }

      if (cmd.hasOption("v")) {
        CliUtils.printVersion();
        return;
      }

      // Explanation: https://github.com/unitycatalog/unitycatalog/pull/398#issuecomment-2325039123
      // tldr: we already have "version" for model_version entity.
      // In the case of no args are provided it is assumed that user wants to see library version;
      // To allow that behaviour version does not require arg, but if it used for entity args should
      // be checked!
      for (Option option : cmd.getOptions()) {
        if (option.getLongOpt().equals("version")) {
          if (cmd.getArgs().length == 0) {
            CliUtils.printVersion();
            return;
          } else {
            if (option.getValue() == null) {
              System.out.println(
                  "Error occurred while parsing the command. Please check the command and try again. Missing argument for option: version");
              CliUtils.printHelp();
              return;
            } else {
              break;
            }
          }
        }
      }

      if (!validateCommand(cmd)) {
        return;
      }

      // Build API client based on the options provided
      ApiClient apiClient = getApiClient(cmd);

      // Check for command and execute the corresponding logic
      String[] subArgs = cmd.getArgs();
      if (subArgs.length < 1) {
        System.out.println("Please provide a command.");
        CliUtils.printHelp();
        return;
      }

      String command = subArgs[0];
      switch (command) {
        case CliUtils.AUTH:
          apiClient.setBasePath("/api/1.0/unity-control");
          AuthCli.handle(cmd, apiClient);
          break;
        case CliUtils.CATALOG:
          CatalogCli.handle(cmd, apiClient);
          break;
        case CliUtils.SCHEMA:
          SchemaCli.handle(cmd, apiClient);
          break;
        case CliUtils.VOLUME:
          VolumeCli.handle(cmd, apiClient);
          break;
        case CliUtils.TABLE:
          TableCli.handle(cmd, apiClient);
          break;
        case CliUtils.FUNCTION:
          FunctionCli.handle(cmd, apiClient);
          break;
        case CliUtils.REGISTERED_MODEL:
          ModelCli.handle(cmd, apiClient);
          break;
        case CliUtils.MODEL_VERSION:
          ModelVersionCli.handle(cmd, apiClient);
          break;
        case CliUtils.PERMISSION:
          PermissionCli.handle(cmd, apiClient);
          break;
        case CliUtils.USER:
          UserCli.handle(cmd, getControlClient(cmd));
          break;
        case CliUtils.METASTORE:
          MetastoreCli.handle(cmd, apiClient);
          break;
        default:
          CliUtils.printHelp();
      }
    } catch (ParseException | JsonProcessingException e) {
      System.out.println(
          "Error occurred while parsing the command. "
              + "Please check the command and try again. "
              + e.getMessage());
      CliUtils.printHelp();
    } catch (CliException e) {
      System.out.println(
          "Error occurred while executing the command. "
              + e.getMessage()
              + (e.getCause() != null ? e.getCause().getMessage() : ""));
    } catch (ApiException | io.unitycatalog.control.ApiException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean validateCommand(CommandLine cmd) {
    String[] subArgs = cmd.getArgs();
    if (subArgs.length < 1) {
      System.out.println("Please provide a entity.");
      CliUtils.printHelp();
      return false;
    } else {
      String entity = cmd.getArgs()[0];
      if (!CliUtils.cliOptions.containsKey(entity.toLowerCase())) {
        System.out.println("Invalid entity provided: " + entity);
        CliUtils.printHelp();
        return false;
      }
      if (subArgs.length < 2) {
        System.out.println("Please provide an operation.");
        CliUtils.printEntityHelp(subArgs[0]);
        return false;
      }
      String operation = cmd.getArgs()[1];
      if (!CliUtils.cliOptions.get(entity.toLowerCase()).containsKey(operation.toLowerCase())) {
        System.out.println("Invalid operation provided: " + operation);
        CliUtils.printEntityHelp(entity);
        return false;
      }
      CliUtils.CliOptions allowedOptions =
          CliUtils.cliOptions.get(entity.toLowerCase()).get(operation.toLowerCase());
      List<CliParams> requiredParams = allowedOptions.getNecessaryParams();
      List<CliParams> optionalParams = allowedOptions.getOptionalParams();
      List<CliParams> specifiedParams = new ArrayList<>();
      for (Option option : cmd.getOptions()) {
        String val = option.getValue();
        if (val == null) {
          System.out.println("Please provide a value for " + option.getLongOpt());
          CliUtils.printSubCommandHelp(entity, operation);
          return false;
        }
        specifiedParams.add(CliParams.fromString(option.getLongOpt()));
      }
      if (!specifiedParams.containsAll(requiredParams)) {
        System.out.println("All required parameters are not provided.");
        CliUtils.printSubCommandHelp(entity, operation);
        return false;
      }
      specifiedParams.removeAll(requiredParams);
      specifiedParams.removeAll(optionalParams);
      specifiedParams.removeAll(commonOptions);
      if (!specifiedParams.isEmpty()) {
        System.out.println("Some of the provided parameters are not valid.");
        CliUtils.printSubCommandHelp(entity, operation);
        return false;
      }
    }
    return true;
  }

  private static String loadProperty(String key, CommandLine cmd) {
    if (cmd.hasOption(key)) {
      return cmd.getOptionValue(key);
    } else if (CliUtils.doesPropertyExist(key)) {
      return CliUtils.getPropertyValue(key);
    }
    return "";
  }

  private static ApiClient getApiClient(CommandLine cmd) {
    // By default, the client will connect to ref server on localhost:8080
    ApiClient apiClient = new ApiClient();
    String server = loadProperty(CliUtils.SERVER, cmd);
    if (server.isEmpty()) {
      server = "http://localhost:8080";
    }
    URI uri = URI.create(server);
    apiClient.setHost(uri.getHost());
    if (uri.getPort() == -1 && uri.getScheme().equals("https")) {
      apiClient.setPort(443);
    } else if (uri.getPort() == -1 && uri.getScheme().equals("http")) {
      apiClient.setPort(8080);
    } else {
      apiClient.setPort(uri.getPort());
    }
    apiClient.setScheme(uri.getScheme());
    String customAuthToken = loadProperty(CliUtils.AUTH_TOKEN, cmd);
    if (!customAuthToken.isEmpty()) {
      apiClient.setRequestInterceptor(
          request -> {
            request.header("Authorization", "Bearer " + customAuthToken);
          });
    }
    return apiClient;
  }

  private static io.unitycatalog.control.ApiClient getControlClient(CommandLine cmd) {
    // By default, the client will connect to ref server on localhost:8080
    io.unitycatalog.control.ApiClient controlClient = new io.unitycatalog.control.ApiClient();
    String server = loadProperty(CliUtils.SERVER, cmd);
    if (server.isEmpty()) {
      server = "http://localhost:8080";
    }
    URI uri = URI.create(server);
    controlClient.setHost(uri.getHost());
    if (uri.getPort() == -1 && uri.getScheme().equals("https")) {
      controlClient.setPort(443);
    } else if (uri.getPort() == -1 && uri.getScheme().equals("http")) {
      controlClient.setPort(8080);
    } else {
      controlClient.setPort(uri.getPort());
    }
    controlClient.setScheme(uri.getScheme());
    String customAuthToken = loadProperty(CliUtils.AUTH_TOKEN, cmd);
    if (!customAuthToken.isEmpty()) {
      controlClient.setRequestInterceptor(
          request -> {
            request.header("Authorization", "Bearer " + customAuthToken);
          });
    }
    return controlClient;
  }
}
