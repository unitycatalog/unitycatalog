package io.unitycatalog.cli.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_FixedWidth;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import io.unitycatalog.client.model.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.fusesource.jansi.AnsiConsole;
import org.json.JSONObject;

public class CliUtils {
  public static final String AUTH = "auth";
  public static final String CATALOG = "catalog";
  public static final String SCHEMA = "schema";
  public static final String VOLUME = "volume";
  public static final String TABLE = "table";
  public static final String METASTORE = "metastore";

  public static final String FUNCTION = "function";
  public static final String REGISTERED_MODEL = "registered_model";
  public static final String MODEL_VERSION = "model_version";
  public static final String PERMISSION = "permission";
  public static final String USER = "user";
  public static final String CREATE = "create";
  public static final String LIST = "list";
  public static final String GET = "get";
  public static final String READ = "read";
  public static final String WRITE = "write";
  public static final String FINALIZE = "finalize";
  public static final String EXECUTE = "call";
  public static final String UPDATE = "update";
  public static final String DELETE = "delete";
  public static final String LOGIN = "login";

  public static final String EMPTY = "";
  public static final String EMPTY_JSON = "{}";

  public static final String SERVER = "server";
  public static final String AUTH_TOKEN = "auth_token";
  public static final String OUTPUT = "output";

  public static final int TABLE_WIDTH = 120;

  public static class CliOptions {
    List<CliParams> necessaryParams;
    List<CliParams> optionalParams;

    public CliOptions(List<CliParams> necessaryParams, List<CliParams> optionalParams) {
      this.necessaryParams = necessaryParams;
      this.optionalParams = optionalParams;
    }

    public List<CliParams> getNecessaryParams() {
      return necessaryParams;
    }

    public List<CliParams> getOptionalParams() {
      return optionalParams;
    }
  }

  public static final Map<String, Map<String, CliOptions>> cliOptions =
      new HashMap<String, Map<String, CliOptions>>() {
        {
          put(
              AUTH,
              new HashMap<String, CliOptions>() {
                {
                  put(LOGIN, new CliOptions(List.of(), List.of(CliParams.IDENTITY_TOKEN)));
                }
              });
          put(
              CATALOG,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.NAME),
                          List.of(CliParams.COMMENT, CliParams.PROPERTIES)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(), List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.NAME), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.NAME),
                          List.of(CliParams.NEW_NAME, CliParams.COMMENT, CliParams.PROPERTIES)));
                  put(DELETE, new CliOptions(List.of(CliParams.NAME), List.of(CliParams.FORCE)));
                }
              });
          put(
              SCHEMA,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME, CliParams.NAME),
                          List.of(CliParams.COMMENT, CliParams.PROPERTIES)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME),
                          List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME),
                          List.of(CliParams.NEW_NAME, CliParams.COMMENT, CliParams.PROPERTIES)));
                  put(
                      DELETE,
                      new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.FORCE)));
                }
              });
          put(
              VOLUME,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME, CliParams.STORAGE_LOCATION),
                          List.of(CliParams.COMMENT)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME, CliParams.SCHEMA_NAME),
                          List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME),
                          List.of(CliParams.COMMENT, CliParams.NEW_NAME)));
                  put(DELETE, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(READ, new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.PATH)));
                  put(WRITE, new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.PATH)));
                }
              });
          put(
              TABLE,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(
                              CliParams.FULL_NAME, CliParams.COLUMNS, CliParams.STORAGE_LOCATION),
                          List.of(CliParams.DATA_SOURCE_FORMAT, CliParams.PROPERTIES)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME, CliParams.SCHEMA_NAME),
                          List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(
                      READ,
                      new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.MAX_RESULTS)));
                  put(WRITE, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(DELETE, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                }
              });
          put(
              FUNCTION,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME, CliParams.INPUT_PARAMS, CliParams.DATA_TYPE),
                          List.of(
                              CliParams.COMMENT,
                              CliParams.ROUTINE_DEFINITION,
                              CliParams.LANGUAGE)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME, CliParams.SCHEMA_NAME),
                          List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(DELETE, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(
                      EXECUTE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME, CliParams.INPUT_PARAMS), List.of()));
                }
              });
          put(
              REGISTERED_MODEL,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.CATALOG_NAME, CliParams.SCHEMA_NAME, CliParams.NAME),
                          List.of(CliParams.COMMENT)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(),
                          List.of(
                              CliParams.CATALOG_NAME,
                              CliParams.SCHEMA_NAME,
                              CliParams.MAX_RESULTS,
                              CliParams.PAGE_TOKEN)));
                  put(GET, new CliOptions(List.of(CliParams.FULL_NAME), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME),
                          List.of(CliParams.NEW_NAME, CliParams.COMMENT)));
                  put(
                      DELETE,
                      new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.FORCE)));
                }
              });
          put(
              MODEL_VERSION,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(
                              CliParams.CATALOG_NAME,
                              CliParams.SCHEMA_NAME,
                              CliParams.NAME,
                              CliParams.SOURCE),
                          List.of(CliParams.COMMENT, CliParams.RUN_ID)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME),
                          List.of(CliParams.MAX_RESULTS, CliParams.PAGE_TOKEN)));
                  put(
                      GET,
                      new CliOptions(List.of(CliParams.FULL_NAME, CliParams.VERSION), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.FULL_NAME, CliParams.VERSION),
                          List.of(CliParams.COMMENT)));
                  put(
                      DELETE,
                      new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.VERSION)));
                  put(
                      FINALIZE,
                      new CliOptions(List.of(CliParams.FULL_NAME), List.of(CliParams.VERSION)));
                }
              });
          put(
              USER,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(CliParams.NAME, CliParams.EMAIL),
                          List.of(CliParams.EXTERNAL_ID)));
                  put(
                      LIST,
                      new CliOptions(
                          List.of(),
                          List.of(CliParams.FILTER, CliParams.START_INDEX, CliParams.COUNT)));
                  put(GET, new CliOptions(List.of(CliParams.ID), List.of()));
                  put(
                      UPDATE,
                      new CliOptions(
                          List.of(CliParams.ID),
                          List.of(CliParams.NAME, CliParams.EXTERNAL_ID, CliParams.EMAIL)));
                  put(DELETE, new CliOptions(List.of(CliParams.ID), List.of()));
                }
              });
          put(
              PERMISSION,
              new HashMap<String, CliOptions>() {
                {
                  put(
                      CREATE,
                      new CliOptions(
                          List.of(
                              CliParams.SECURABLE_TYPE,
                              CliParams.NAME,
                              CliParams.PRINCIPAL,
                              CliParams.PRIVILEGE),
                          List.of()));
                  put(
                      DELETE,
                      new CliOptions(
                          List.of(
                              CliParams.SECURABLE_TYPE,
                              CliParams.NAME,
                              CliParams.PRINCIPAL,
                              CliParams.PRIVILEGE),
                          List.of()));
                  put(
                      GET,
                      new CliOptions(
                          List.of(CliParams.SECURABLE_TYPE, CliParams.NAME),
                          List.of(CliParams.PRINCIPAL)));
                }
              });
          put(
              METASTORE,
              new HashMap<String, CliOptions>() {
                {
                  put(GET, new CliOptions(List.of(), List.of()));
                }
              });
        }
      };

  public static final List<CliParams> commonOptions =
      Arrays.asList(CliParams.SERVER, CliParams.AUTH_TOKEN, CliParams.OUTPUT);
  private static final Properties properties = new Properties();

  static {
    try (InputStream input =
        CliUtils.class.getClassLoader().getResourceAsStream("application.properties")) {
      properties.load(input);
    } catch (IOException e) {
      System.out.println("Error while loading properties file.");
    }
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectWriter objectWriter;

  public static ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  public static ObjectWriter getObjectWriter(CommandLine cmd) {
    if (objectWriter == null) {
      if (cmd.hasOption(OUTPUT) && "jsonPretty".equals(cmd.getOptionValue(OUTPUT))) {
        objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
      } else {
        objectWriter = objectMapper.writer();
      }
    }
    return objectWriter;
  }

  public static String preprocess(String value, int length) {
    if (value.length() > length && length > 3) {
      return value.substring(0, length - 3) + "...";
    }
    return value;
  }

  public static List<String> getFieldNames(JsonNode element) {
    List<String> columns = new ArrayList<>();
    if (element.isObject()) {
      (element).fieldNames().forEachRemaining(columns::add);
    }
    return columns;
  }

  /**
   * Generate the maximum lengths of the columns values in the given JSON node.
   *
   * @param node The JSON node representing a list of nodes.
   * @param fieldNames The column names.
   * @param columnWidths Updated with the maximum length of each column.
   * @param isFixedWidthColumns Updated with whether the column is fixed width or not.
   * @return the maximum length of the column values.
   */
  private static int calculateFieldWidths(
      JsonNode node, List<String> fieldNames, int[] columnWidths, boolean[] isFixedWidthColumns) {
    int maxLineWidth = 0;

    for (int columnIndex = 0; columnIndex < fieldNames.size(); columnIndex++) {
      columnWidths[columnIndex] = fieldNames.get(columnIndex).length();
    }

    for (JsonNode jsonNode : node) {
      int lineWidth = 1;
      for (int columnIndex = 0; columnIndex < fieldNames.size(); columnIndex++) {
        String fieldName = fieldNames.get(columnIndex);
        JsonNode value = jsonNode.get(fieldName);
        String valueString = value.isTextual() ? value.asText() : value.toString();

        columnWidths[columnIndex] = Math.max(columnWidths[columnIndex], valueString.length());
        lineWidth += valueString.length() + 1;

        String fieldNameLowerCase = fieldName.toLowerCase();
        if (fieldNameLowerCase.equals("name") || fieldNameLowerCase.endsWith("id")) {
          isFixedWidthColumns[columnIndex] = true;
        }
      }
      maxLineWidth = Math.max(maxLineWidth, lineWidth);
    }

    return maxLineWidth;
  }

  /**
   * Adjust the column widths to fit the output width, ignore fixed width columns.
   *
   * @param fieldNames The column names.
   * @param outputWidth The output width.
   * @param columnWidths The column widths, updated with the new widths to fix the output width.
   * @param isFixedWidthColumns Whether the column is fixed width or not.
   */
  private static void adjustColumnWidths(
      List<String> fieldNames, int outputWidth, int[] columnWidths, boolean[] isFixedWidthColumns) {

    int[] fixedWidthIndices =
        IntStream.range(0, fieldNames.size()).filter(idx -> isFixedWidthColumns[idx]).toArray();

    int widthRemaining =
        outputWidth
            - fieldNames.size()
            - IntStream.of(fixedWidthIndices).map(idx -> columnWidths[idx]).sum();

    int columnWidth = (widthRemaining) / (fieldNames.size() - fixedWidthIndices.length);

    IntStream.range(0, fieldNames.size())
        .filter(idx -> !isFixedWidthColumns[idx])
        .forEach(idx -> columnWidths[idx] = columnWidth);
  }

  private static void processOutputAsRows(AsciiTable at, JsonNode node, int outputWidth) {
    List<String> fieldNames = getFieldNames(node.get(0));

    int[] columnWidths = new int[fieldNames.size()];
    boolean[] isFixedWidthColumns = new boolean[fieldNames.size()];
    int maxLineWidth = calculateFieldWidths(node, fieldNames, columnWidths, isFixedWidthColumns);

    if (maxLineWidth >= outputWidth) {
      adjustColumnWidths(fieldNames, outputWidth, columnWidths, isFixedWidthColumns);
    }

    List<String> headers =
        fieldNames.stream().map(String::toUpperCase).collect(Collectors.toList());

    CWC_FixedWidth cwc = new CWC_FixedWidth();
    for (int i = 0; i < fieldNames.size(); i++) {
      cwc.add(columnWidths[i]);
    }

    at.getRenderer().setCWC(cwc);
    at.addRule();
    at.addRow(headers.toArray()).setTextAlignment(TextAlignment.CENTER);
    at.addRule();
    node.forEach(
        element -> {
          List<String> row = new ArrayList<>();
          ObjectNode objectNode = (ObjectNode) element;
          Iterator<String> nodeFieldNames = objectNode.fieldNames();
          int columnIndex = 0;
          while (nodeFieldNames.hasNext()) {
            String nodeFieldName = nodeFieldNames.next();
            JsonNode value = element.get(nodeFieldName);
            String valueString = value.isTextual() ? value.asText() : value.toString();
            row.add(preprocess(valueString, columnWidths[columnIndex]));
            columnIndex++;
          }
          at.addRow(row.toArray());
          at.addRule();
        });
  }

  private static void processOutputAsKeysAndValues(AsciiTable at, JsonNode node, int outputWidth) {
    int minOutputWidth = outputWidth / 2;

    at.getRenderer()
        .setCWC(
            new CWC_LongestLine()
                .add(minOutputWidth / 3, outputWidth / 4)
                .add(2 * minOutputWidth / 3, 3 * outputWidth / 4));
    at.addRule();
    at.addRow("KEY", "VALUE").setTextAlignment(TextAlignment.CENTER);
    at.addRule();
    node.fields()
        .forEachRemaining(
            field -> {
              JsonNode value = field.getValue();
              StringBuilder valueString = new StringBuilder();
              if (value.isTextual()) {
                valueString = new StringBuilder(value.asText());
              } else if (value.isArray()) {
                ArrayNode arrayNode = (ArrayNode) value;
                for (int i = 0; i < arrayNode.size(); i++) {
                  valueString.append(arrayNode.get(i).toString()).append("\n\n");
                }
              } else {
                valueString = new StringBuilder(value.toString());
              }
              at.addRow(field.getKey().toUpperCase(), valueString.toString())
                  .setTextAlignment(TextAlignment.LEFT);
              at.addRule();
            });
  }

  private static int getOutputWidth() {
    // TODO: maybe turn this into a command line parameter.
    String envWidth = System.getenv().get("UC_OUTPUT_WIDTH");
    if (envWidth != null) {
      return Integer.parseInt(envWidth);
    } else {
      return Math.max(TABLE_WIDTH, AnsiConsole.getTerminalWidth());
    }
  }

  public static void postProcessAndPrintOutput(CommandLine cmd, String output, String subCommand) {
    boolean jsonFormat =
        cmd.hasOption(OUTPUT)
            && ("json".equals(cmd.getOptionValue(OUTPUT))
                || "jsonPretty".equals(cmd.getOptionValue(OUTPUT)));
    if (jsonFormat || READ.equals(subCommand) || EXECUTE.equals(subCommand)) {
      System.out.println(output);
    } else {
      AsciiTable at = new AsciiTable();
      int outputWidth = getOutputWidth();
      try {
        JsonNode node = objectMapper.readTree(output);
        if (node.isArray()) {
          if (node.isEmpty()) {
            System.out.println(output);
            return;
          } else {
            processOutputAsRows(at, node, outputWidth);
          }
        } else {
          processOutputAsKeysAndValues(at, node, outputWidth);
        }
        System.out.println(at.render(outputWidth));
      } catch (Exception e) {
        System.out.println("Error while printing output as table: " + e.getMessage());
        System.out.println(output);
      }
    }
  }

  public static JSONObject createJsonFromOptions(CommandLine cmd) {
    JSONObject json = new JSONObject();
    for (Option opt : cmd.getOptions()) {
      // Skip server related options
      if (Arrays.asList(SERVER, AUTH_TOKEN, OUTPUT).contains(opt.getLongOpt())) {
        continue;
      }
      String optValue = opt.getValue();
      if (optValue != null) {
        json.put(CliParams.fromString(opt.getLongOpt()).getServerParam(), optValue);
      }
    }
    return json;
  }

  public static void printEntityHelp(String entity) {
    Map<String, CliOptions> entityOptions = cliOptions.get(entity);
    System.out.printf("Please provide a valid sub-command for %s.\n", entity);
    System.out.printf(
        "Valid sub-commands for %s are: %s\n", entity, String.join(", ", entityOptions.keySet()));
    System.out.printf(
        "For detailed help on %s sub-commands, use bin/uc %s <sub-command> --help\n",
        entity, entity);
  }

  public static void printSubCommandHelp(String entity, String operation) {
    CliOptions options = cliOptions.get(entity).get(operation);
    System.out.printf("Usage: bin/uc %s %s [options]\n", entity, operation);
    System.out.println("Required Params:");
    for (CliParams param : options.getNecessaryParams()) {
      System.out.printf("  --%s %s\n", param.val(), param.getHelpMessage());
    }
    System.out.println("Optional Params:");
    for (CliParams param : commonOptions) {
      System.out.printf("  --%s %s\n", param.val(), param.getHelpMessage());
    }
    for (CliParams param : options.getOptionalParams()) {
      System.out.printf("  --%s %s\n", param.val(), param.getHelpMessage());
    }
  }

  public static void printHelp() {
    System.out.println();
    System.out.println("Usage: bin/uc <entity> <operation> [options]");
    System.out.printf("Entities: %s\n", String.join(", ", cliOptions.keySet()));
    System.out.println();
    System.out.println(
        "By default, the client will connect to UC running locally at http://localhost:8080\n");
    System.out.println("To connect to specific UC server, use --server https://<host>:<port>\n");
    System.out.println(
        "Currently, auth using bearer token is supported. Please specify the token via --auth_token"
            + " <PAT Token>\n");
    System.out.println(
        "For detailed help on entity specific operations, use bin/uc <entity> --help");
  }

  public static void printVersion() {
    System.out.println(VersionUtils.VERSION);
  }

  public static List<String> parseFullName(String fullName) {
    return Arrays.asList(fullName.split("\\."));
  }

  public static String getPropertyValue(String key) {
    String value = properties.getProperty(key);
    if (value.isEmpty()) return null;
    return value;
  }

  public static void setProperty(String key, String value) {
    properties.setProperty(key, value);
  }

  public static boolean doesPropertyExist(String key) {
    return properties.containsKey(key) && !properties.getProperty(key).isEmpty();
  }

  public static void resolveFullNameToThreeLevelNamespace(JSONObject json) {
    String fullName = json.getString(CliParams.FULL_NAME.getServerParam());
    List<String> threeLevelNamespace = CliUtils.parseFullName(fullName);
    if (threeLevelNamespace.size() != 3) {
      printSubCommandHelp(TABLE, CREATE);
      throw new RuntimeException("Full name is not three level namespace: " + fullName);
    }
    // Set fields in json object for seamless deserialization
    json.put(CliParams.CATALOG_NAME.getServerParam(), threeLevelNamespace.get(0));
    json.put(CliParams.SCHEMA_NAME.getServerParam(), threeLevelNamespace.get(1));
    json.put(CliParams.NAME.getServerParam(), threeLevelNamespace.get(2));
    json.remove(CliParams.FULL_NAME.getServerParam());
  }

  public static Map<String, String> extractProperties(ObjectMapper objectMapper, JSONObject node) {
    // Retrieve the escaped JSON string
    if (node.has(CreateTable.JSON_PROPERTY_PROPERTIES)) {
      String escapedJson = node.get(CreateTable.JSON_PROPERTY_PROPERTIES).toString();
      // Parse the escaped JSON string into a JsonNode
      try {
        JsonNode propertiesNode = objectMapper.readTree(escapedJson);
        TypeFactory typeFactory = objectMapper.getTypeFactory();

        // Convert the JsonNode to a Map
        return objectMapper.convertValue(
            propertiesNode, typeFactory.constructMapType(Map.class, String.class, String.class));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to deserialize properties field " + escapedJson, e);
      }
    }
    return null;
  }

  private static String setPrecisionAndScale(
      ColumnInfo columnInfo, String typeText, String params) {
    if (params != null) {
      String[] paramArray = params.split(",");
      if (typeText.equals("DECIMAL") && paramArray.length == 2) {
        columnInfo.setTypePrecision(Integer.valueOf(paramArray[0].trim()));
        columnInfo.setTypeScale(Integer.valueOf(paramArray[1].trim()));
        return "(" + paramArray[0].trim() + "," + paramArray[1].trim() + ")";
      }
    } else {
      columnInfo.setTypePrecision(0);
      columnInfo.setTypeScale(0);
    }
    return "";
  }

  /**
   * Method to convert the columns string to a list of ColumnInfo objects the columns string is
   * expected to be in the format: column1 type1, column2 type2, ... e.g., id INT, name String, age
   * INT
   *
   * @param columnsString
   * @return
   */
  public static List<ColumnInfo> parseColumns(String columnsString) {
    List<ColumnInfo> columns = new ArrayList<>();
    Pattern pattern = Pattern.compile("(\\w+)\\s+(\\w+)(\\(([^)]+)\\))?");
    Matcher matcher = pattern.matcher(columnsString);
    int position = 0;

    while (matcher.find()) {
      try {
        ColumnInfo columnInfo = new ColumnInfo();

        String name = matcher.group(1); // e.g., column name
        String typeName = matcher.group(2).toUpperCase(); // e.g., DECIMAL, VARCHAR, INT
        String params = matcher.group(4); // e.g., 10,2 or 255

        columnInfo.setName(name);
        columnInfo.setTypeText(typeNameVsTypeText.getOrDefault(typeName, typeName.toLowerCase()));
        columnInfo.setTypeName(ColumnTypeName.valueOf(typeName));
        columnInfo.setTypeJson(
            String.format(
                "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
                name,
                typeNameVsJsonText.getOrDefault(typeName, typeName.toLowerCase(Locale.ROOT))
                    + setPrecisionAndScale(columnInfo, typeName, params),
                "true"));
        columnInfo.setPosition(position++);
        columns.add(columnInfo);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid column definition: " + matcher.group(0), e);
      }
    }

    return columns;
  }

  static Map<String, String> typeNameVsTypeText =
      new HashMap<String, String>() {
        {
          put("LONG", "bigint");
          put("SHORT", "smallint");
          put("BYTE", "tinyint");
        }
      };

  static Map<String, String> typeNameVsJsonText =
      new HashMap<String, String>() {
        {
          put("INT", "integer");
        }
      };

  /*
  Input parameters would be given in the form
  --input_params "param1 INT, param2 STRING"
   */
  public static FunctionParameterInfos parseInputParams(JSONObject json) {
    String[] inputParams = json.getString(CliParams.INPUT_PARAMS.getServerParam()).split(",");
    List<FunctionParameterInfo> parameterInfos = new ArrayList<>();
    for (int i = 0; i < inputParams.length; i++) {
      String name = inputParams[i].trim().split(" ")[0];
      String typeName = (inputParams[i].trim().split(" ")[1]).toUpperCase();
      FunctionParameterInfo param =
          new FunctionParameterInfo()
              .name(name)
              .typeName(ColumnTypeName.valueOf(typeName))
              .typeText(typeNameVsTypeText.getOrDefault(typeName, typeName.toLowerCase()))
              .typeJson(
                  String.format(
                      "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
                      name,
                      typeNameVsJsonText.getOrDefault(typeName, typeName.toLowerCase()),
                      "true"))
              .position(i);
      parameterInfos.add(param);
    }
    json.remove(CliParams.INPUT_PARAMS.getServerParam());
    return new FunctionParameterInfos().parameters(parameterInfos);
  }
}
