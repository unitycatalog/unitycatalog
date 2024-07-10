package io.unitycatalog.server.utils;

import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ColumnUtils {

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

  static String getTypeText(ColumnTypeName typeName) {
    return typeNameVsTypeText.getOrDefault(
        typeName.getValue(), typeName.getValue().toLowerCase(Locale.ROOT));
  }

  static String getTypeJson(
      ColumnTypeName typeName, String columnName, boolean isNullable, String precisionAndScale) {

    return String.format(
        "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
        columnName,
        typeNameVsJsonText.getOrDefault(
                typeName.getValue(), typeName.getValue().toLowerCase(Locale.ROOT))
            + (precisionAndScale != null ? precisionAndScale : ""),
        isNullable);
  }

  static String getPrecisionAndScale(ColumnInfoDAO columnInfoDAO) {
    if (columnInfoDAO.getTypePrecision() == null || columnInfoDAO.getTypeScale() == null) {
      columnInfoDAO.setTypePrecision(0);
      columnInfoDAO.setTypeScale(0);
      return "";
    }
    return getPrecisionAndScale(columnInfoDAO.getTypePrecision(), columnInfoDAO.getTypeScale());
  }

  public static String getPrecisionAndScale(int precision, int scale) {
    return "(" + precision + "," + scale + ")";
  }

  public static void addTypeTextAndJsonText(ColumnInfoDAO columnInfoDAO) {
    String typeName = columnInfoDAO.getTypeName();
    columnInfoDAO.setTypeText(
        typeNameVsTypeText.getOrDefault(typeName, typeName.toLowerCase(Locale.ROOT)));
    columnInfoDAO.setTypeJson(
        getTypeJson(
            ColumnTypeName.valueOf(columnInfoDAO.getTypeName()),
            columnInfoDAO.getName(),
            columnInfoDAO.isNullable(),
            getPrecisionAndScale(columnInfoDAO)));
  }
}
