package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.commonOptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.core.config.Configurator;

public class UnityCatalogCli {
  private static final String UC_BASE_PATH = "/api/2.1/unity-catalog";
  private static final String CONTROL_BASE_PATH = "/api/1.0/unity-control";
  private static final Pattern SERVER_PATH_SUFFIX =
      Pattern.compile(
          "/*(" + Pattern.quote(UC_BASE_PATH) + "|" + Pattern.quote(CONTROL_BASE_PATH) + ")?/*$");

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
                  "Error occurred while parsing the command. Please check the command and try"
                      + " again. Missing argument for option: version");
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
          // The auth endpoints are part of the control API, not the UC API.
          apiClient.updateBaseUri(getBaseUri(cmd, CONTROL_BASE_PATH));
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
          TableCli.handle(cmd, apiClient, loadProperty(CliUtils.AUTH_TOKEN, cmd));
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
        case CliUtils.CREDENTIAL:
          CredentialCli.handle(cmd, apiClient);
          break;
        case CliUtils.EXTERNAL_LOCATION:
          ExternalLocationCli.handle(cmd, apiClient);
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
    ApiClient apiClient = new ApiClient();
    apiClient.updateBaseUri(getBaseUri(cmd, UC_BASE_PATH));
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
    io.unitycatalog.control.ApiClient controlClient = new io.unitycatalog.control.ApiClient();
    controlClient.updateBaseUri(getBaseUri(cmd, CONTROL_BASE_PATH));
    String customAuthToken = loadProperty(CliUtils.AUTH_TOKEN, cmd);
    if (!customAuthToken.isEmpty()) {
      controlClient.setRequestInterceptor(
          request -> {
            request.header("Authorization", "Bearer " + customAuthToken);
          });
    }
    return controlClient;
  }

  /**
   * Resolves the URI of one API of the server given by {@code --server} or the {@code server}
   * property. Without a server, the CLI connects to the reference server on {@code localhost:8080}.
   * Without a port, it uses 443 for https and 8080 for http.
   *
   * @param cmd the parsed command line
   * @param basePath the base path of the API, such as {@code /api/2.1/unity-catalog}
   * @return the server URL with its port and the base path appended to its path
   */
  private static String getBaseUri(CommandLine cmd, String basePath) {
    String server = loadProperty(CliUtils.SERVER, cmd);
    URI uri = URI.create(server.isEmpty() ? "http://localhost:8080" : server);
    int port = uri.getPort();
    if (port == -1 && uri.getScheme().equals("https")) {
      port = 443;
    } else if (port == -1 && uri.getScheme().equals("http")) {
      port = 8080;
    }
    return uri.getScheme()
        + "://"
        + uri.getHost()
        + (port == -1 ? "" : ":" + port)
        + getServerPath(uri)
        + basePath;
  }

  /**
   * Returns the path of the server URL without trailing slashes or a trailing API base path, so
   * that a server behind a gateway path such as {@code https://gateway.example.com/uc} is reached
   * at {@code /uc/api/...}, and a URL that already ends in {@code /api/2.1/unity-catalog} or {@code
   * /api/1.0/unity-control} works for every API.
   *
   * @param uri the server URL
   * @return the raw path, which is empty when the URL has none
   */
  private static String getServerPath(URI uri) {
    return SERVER_PATH_SUFFIX
        .matcher(Objects.requireNonNullElse(uri.getRawPath(), ""))
        .replaceFirst("");
  }
}
