package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.MetastoresApi;
import org.apache.commons.cli.CommandLine;

public class MetastoreCli {
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    MetastoresApi metastoresApi = new MetastoresApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.GET:
        output = objectWriter.writeValueAsString(metastoresApi.summary());
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.METASTORE);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }
}
