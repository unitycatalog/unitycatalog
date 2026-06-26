package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class ColumnModels {

  private ColumnModels() {}

  public static ColumnInfo column(
      String name, ColumnTypeName typeName, String typeText, boolean nullable, int position) {
    String sparkType = sparkTypeFor(typeText);
    String typeJson =
        String.format(
            "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
            name, sparkType, nullable);
    return new ColumnInfo()
        .name(name)
        .typeName(typeName)
        .typeText(typeText)
        .typeJson(typeJson)
        .position(position)
        .nullable(nullable);
  }

  public static List<ColumnInfo> columns(ColumnInfo... cols) {
    return List.of(cols);
  }

  public static List<ColumnInfo> parseColumns(List<String> specs) {
    List<ColumnInfo> columns = new ArrayList<>();
    for (int i = 0; i < specs.size(); i++) {
      columns.add(parseColumn(specs.get(i), i));
    }
    return columns;
  }

  private static ColumnInfo parseColumn(String spec, int position) {
    String[] parts = spec.split(":", 4);
    if (parts.length != 4) {
      throw new IllegalArgumentException(
          "Invalid column spec '" + spec + "'; expected name:TYPE_NAME:type_text:nullable");
    }
    return column(
        parts[0],
        ColumnTypeName.fromValue(parts[1]),
        parts[2],
        Boolean.parseBoolean(parts[3]),
        position);
  }

  private static String sparkTypeFor(String typeText) {
    return switch (typeText.toLowerCase(Locale.ROOT)) {
      case "int", "integer" -> "integer";
      case "bigint", "long" -> "long";
      case "string" -> "string";
      case "double" -> "double";
      case "float" -> "float";
      case "boolean" -> "boolean";
      default -> typeText.toLowerCase(Locale.ROOT);
    };
  }
}
