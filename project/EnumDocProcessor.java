import sbt.util.Logger;
import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.Supplier;

/**
 * Post-processes OpenAPI-generated markdown documentation to add enum value tables.
 *
 * <p>Generates tables for ALL enums: - If x-enum-descriptions exists, creates a table with values
 * and descriptions - If no descriptions, creates a simple list of enum values
 */
public class EnumDocProcessor {
  File specFile;
  File modelsDir;
  Logger logger;

  /**
   * Process all enum schemas in the OpenAPI spec and update their markdown files.
   *
   * @param specFile The OpenAPI specification file (all.yaml)
   * @param modelsDir The directory containing generated markdown files (Models/)
   * @param logger The SBT logger for output
   */
  public EnumDocProcessor(File specFile, File modelsDir, Logger logger) {
    this.specFile = specFile;
    this.modelsDir = modelsDir;
    this.logger = logger;
  }

  // Logging wrapper methods to avoid verbose Supplier casting
  private void logInfo(String message) {
    logger.info((Supplier<String>) () -> message);
  }

  private void logDebug(String message) {
    logger.debug((Supplier<String>) () -> message);
  }

  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    if (!specFile.exists()) {
      throw new RuntimeException("OpenAPI spec file not found: " + specFile);
    }

    if (!modelsDir.exists()) {
      throw new RuntimeException("Models directory not found: " + modelsDir);
    }

    logInfo("Processing enum documentation...");

    try (FileInputStream inputStream = new FileInputStream(specFile)) {
      Yaml yaml = new Yaml();
      Map<String, Object> data = yaml.load(inputStream);

      // Navigate to components.schemas
      Map<String, Object> components = (Map<String, Object>) data.get("components");
      if (components == null) {
        throw new RuntimeException("Could not find 'components' in OpenAPI spec");
      }

      Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
      if (schemas == null) {
        throw new RuntimeException("Could not find 'schemas' in OpenAPI spec");
      }

      int processedCount = 0;
      for (Map.Entry<String, Object> entry : schemas.entrySet()) {
        String schemaName = entry.getKey();
        Object schemaDef = entry.getValue();
        Optional<EnumSchema> enumSchema = parseEnumSchema(schemaName, schemaDef);
        if (enumSchema.isPresent() && updateMarkdownFile(schemaName, enumSchema.get()))
          processedCount++;
      }
      logInfo("Processed " + processedCount + " enum schema(s)");
      return processedCount;
    }
  }

  /** Inner class to hold enum schema information */
  private static class EnumSchema {
    final String typeDescription;
    final List<String> enumValues;
    final List<String> enumDescriptions;

    EnumSchema(String typeDescription, List<String> enumValues, List<String> enumDescriptions) {
      this.typeDescription = typeDescription;
      this.enumValues = enumValues;
      this.enumDescriptions = enumDescriptions;
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<EnumSchema> parseEnumSchema(String schemaName, Object schemaDef) {
    if (!(schemaDef instanceof Map)) {
      return Optional.empty();
    }

    Map<String, Object> schema = (Map<String, Object>) schemaDef;

    // Check if this is an enum
    Object enumObj = schema.get("enum");
    if (enumObj == null) {
      return Optional.empty(); // Not an enum
    }
    List<String> enumValues = (List<String>) enumObj;

    // Extract the enum type description
    String typeDescription = (String) schema.get("description");
    if (typeDescription == null) {
      typeDescription = "";
    }

    // Check for descriptions
    Object descriptionsObj = schema.get("x-enum-descriptions");
    if (descriptionsObj == null) {
      // Does not have descriptions. That's OK.
      return Optional.of(new EnumSchema(typeDescription, enumValues, List.of()));
    }

    List<String> descriptionList = (List<String>) descriptionsObj;
    // Enforce all-or-nothing: if descriptions exist, must have one for each value
    if (descriptionList.size() != enumValues.size()) {
      throw new RuntimeException(
          String.format(
              "%s has %d enum values but %d descriptions.",
              schemaName, enumValues.size(), descriptionList.size()));
    }

    List<String> cleanDescriptionList = new ArrayList<>();
    for (int i = 0; i < descriptionList.size(); i++) {
      String enumValue = enumValues.get(i);
      String desc = descriptionList.get(i);
      String expectedPrefix = enumValue + ": ";

      // Validate that description starts with enum value. This is to make sure that the
      // descriptions are in proper order.
      if (!desc.startsWith(expectedPrefix)) {
        throw new RuntimeException(
            String.format(
                "%s.%s: Description must start with '%s' but got: '%s'",
                schemaName, enumValue, expectedPrefix, desc));
      }

      // Strip the prefix and store the clean description
      String cleanDescription = desc.substring(expectedPrefix.length());
      cleanDescriptionList.add(cleanDescription);
    }
    return Optional.of(new EnumSchema(typeDescription, enumValues, cleanDescriptionList));
  }

  /**
   * Update the markdown file for an enum schema with a table of values (and optionally
   * descriptions).
   */
  private boolean updateMarkdownFile(String schemaName, EnumSchema enumSchema) throws IOException {

    File mdFile = new File(modelsDir, schemaName + ".md");
    if (!mdFile.exists()) {
      throw new RuntimeException("Markdown file not found: " + mdFile);
    }

    // Read the existing content
    String content = new String(Files.readAllBytes(mdFile.toPath()));

    // Check if already processed
    if (content.contains("## Enum Values")) {
      logDebug("Skipping " + schemaName + " - already processed");
      return false;
    }

    // Add the enum type description after the title
    if (!enumSchema.typeDescription.isEmpty()) {
      String titleMarker = "# " + schemaName + "\n";
      int titlePos = content.indexOf(titleMarker);
      if (titlePos == -1) {
        throw new RuntimeException("Could not find title position in " + mdFile);
      }
      int insertPos = titlePos + titleMarker.length();
      content =
          content.substring(0, insertPos)
              + "\n"
              + enumSchema.typeDescription
              + "\n\n"
              + content.substring(insertPos);
    }

    // Build the enum values table
    StringBuilder enumTable = new StringBuilder("\n## Enum Values\n\n");

    if (!enumSchema.enumDescriptions.isEmpty()) {
      // Table with descriptions
      enumTable.append("| Value | Description |\n");
      enumTable.append("|-------|-------------|\n");

      for (int i = 0; i < enumSchema.enumValues.size(); i++) {
        String value = enumSchema.enumValues.get(i);
        String description = enumSchema.enumDescriptions.get(i);
        // Escape pipe characters in descriptions
        String safeDescription = description.replace("|", "\\|");
        enumTable.append("| `").append(value).append("` | ").append(safeDescription).append(" |\n");
      }
    } else {
      // Simple list without descriptions
      enumTable.append("| Value |\n");
      enumTable.append("|-------|\n");

      for (String value : enumSchema.enumValues) {
        enumTable.append("| `").append(value).append("` |\n");
      }
    }
    enumTable.append("\n");

    // Find insertion point (before the back links)
    String backLinkMarker = "[[Back to Model list]]";
    int insertPos = content.indexOf(backLinkMarker);
    if (insertPos == -1) {
      throw new RuntimeException("Could not find insertion point in " + mdFile);
    }

    // Insert the enum table before the back links
    content = content.substring(0, insertPos) + enumTable + content.substring(insertPos);

    // Write the updated content
    Files.write(mdFile.toPath(), content.getBytes());

    String withDescriptionsText =
        enumSchema.enumDescriptions.isEmpty() ? "" : " (with descriptions)";
    logInfo("✓ Updated " + schemaName + ".md" + withDescriptionsText);
    return true;
  }
}
