package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.ListCredentialsResponse;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class CredentialCli {

  private static final String AZURE_CLIENT_SECRET_ENV = "UC_AZURE_CLIENT_SECRET";
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    CredentialsApi credentialsApi = new CredentialsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createCredential(credentialsApi, json);
        break;
      case CliUtils.LIST:
        output = listCredentials(credentialsApi, json);
        break;
      case CliUtils.GET:
        output = getCredential(credentialsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateCredential(credentialsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteCredential(credentialsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.CREDENTIAL);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static Optional<AwsIamRoleRequest> extractAwsIamRole(JSONObject json) {
    if (json.has(CliParams.AWS_IAM_ROLE_ARN.getServerParam())) {
      String awsIamRoleArnStr = json.getString(CliParams.AWS_IAM_ROLE_ARN.getServerParam());
      return Optional.of(new AwsIamRoleRequest().roleArn(awsIamRoleArnStr));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<AzureServicePrincipalRequest> extractAzureServicePrincipal(
      JSONObject json) {
    List<CliParams> params =
        List.of(
            CliParams.AZURE_DIRECTORY_ID,
            CliParams.AZURE_APPLICATION_ID,
            CliParams.AZURE_CLIENT_SECRET);
    if (params.stream().noneMatch(param -> json.has(param.getServerParam()))) {
      return Optional.empty();
    }
    if (!json.has(CliParams.AZURE_DIRECTORY_ID.getServerParam())
        || !json.has(CliParams.AZURE_APPLICATION_ID.getServerParam())) {
      throw new CliException(
          "An Azure credential needs both --azure_directory_id and --azure_application_id.");
    }
    return Optional.of(
        new AzureServicePrincipalRequest()
            .directoryId(json.getString(CliParams.AZURE_DIRECTORY_ID.getServerParam()))
            .applicationId(json.getString(CliParams.AZURE_APPLICATION_ID.getServerParam()))
            .clientSecret(
                azureClientSecret(
                    json.optString(CliParams.AZURE_CLIENT_SECRET.getServerParam(), null),
                    System.getenv(),
                    CredentialCli::promptForAzureClientSecret)));
  }

  /**
   * The client secret from {@code --azure_client_secret}, else from {@code UC_AZURE_CLIENT_SECRET},
   * else typed at a prompt that does not echo it. The variable and the prompt keep the secret out
   * of shell history and the process list.
   *
   * @param prompt returns the typed secret, or null when there is no terminal to prompt on
   */
  public static String azureClientSecret(
      String flag, Map<String, String> env, Supplier<char[]> prompt) {
    if (flag != null) {
      return flag;
    }
    // Trailing line breaks are dropped, as for UC_SERVER_CLIENT_SECRET.
    String fromEnv = env.getOrDefault(AZURE_CLIENT_SECRET_ENV, "").replaceAll("[\\r\\n]+$", "");
    if (!fromEnv.isEmpty()) {
      return fromEnv;
    }
    char[] typed = prompt.get();
    if (typed == null) {
      throw new CliException(
          "Set the Azure client secret in "
              + AZURE_CLIENT_SECRET_ENV
              + " or with --azure_client_secret, or run the command in a terminal to be prompted"
              + " for it.");
    }
    return new String(typed);
  }

  private static char[] promptForAzureClientSecret() {
    Console console = System.console();
    return console == null ? null : console.readPassword("Azure client secret: ");
  }

  /** Reads the key from a service account's JSON key file, as downloaded from Google Cloud. */
  private static Optional<GcpServiceAccountKeyRequest> extractGcpServiceAccountKey(
      JSONObject json) {
    if (!json.has(CliParams.GCP_SERVICE_ACCOUNT_KEY_FILE.getServerParam())) {
      return Optional.empty();
    }
    String path = json.getString(CliParams.GCP_SERVICE_ACCOUNT_KEY_FILE.getServerParam());
    JsonNode keyFile;
    try {
      keyFile = new ObjectMapper().readTree(new File(path));
    } catch (IOException e) {
      throw new CliException("Cannot read the service account key file " + path, e);
    }
    return Optional.of(
        new GcpServiceAccountKeyRequest()
            .email(keyFileField(keyFile, "client_email", path))
            .privateKeyId(keyFileField(keyFile, "private_key_id", path))
            .privateKey(keyFileField(keyFile, "private_key", path)));
  }

  private static String keyFileField(JsonNode keyFile, String field, String path) {
    JsonNode value = keyFile.get(field);
    if (value == null || !value.isTextual()) {
      throw new CliException(
          String.format("%s is not a service account key file: it has no %s.", path, field));
    }
    return value.asText();
  }

  private static String createCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(extractAwsIamRole(json).orElse(null))
            .azureServicePrincipal(extractAzureServicePrincipal(json).orElse(null))
            .gcpServiceAccountKey(extractGcpServiceAccountKey(json).orElse(null));
    CredentialInfo credentialInfo = credentialsApi.createCredential(createCredentialRequest);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String listCredentials(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    int maxResults = json.optInt(CliParams.MAX_RESULTS.getServerParam(), 100);
    String pageToken = json.optString(CliParams.PAGE_TOKEN.getServerParam(), null);
    ListCredentialsResponse listCredentialsResponse =
        credentialsApi.listCredentials(maxResults, pageToken, null);
    List<CredentialInfo> credentials = listCredentialsResponse.getCredentials();
    return objectWriter.writeValueAsString(credentials);
  }

  private static String getCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    CredentialInfo credentialInfo = credentialsApi.getCredential(name);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String updateCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    json.remove(CliParams.NAME.getServerParam());
    CliUtils.validateUpdateParameters(json, CliUtils.CREDENTIAL, CliUtils.UPDATE);
    UpdateCredentialRequest updateCredentialRequest =
        new UpdateCredentialRequest()
            .newName(json.optString(CliParams.NEW_NAME.getServerParam(), null))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .awsIamRole(extractAwsIamRole(json).orElse(null));
    CredentialInfo credentialInfo = credentialsApi.updateCredential(name, updateCredentialRequest);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String deleteCredential(CredentialsApi credentialsApi, JSONObject json)
      throws ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    boolean force = json.optBoolean(CliParams.FORCE.getServerParam(), false);
    credentialsApi.deleteCredential(name, force);
    return CliUtils.EMPTY_JSON;
  }
}
