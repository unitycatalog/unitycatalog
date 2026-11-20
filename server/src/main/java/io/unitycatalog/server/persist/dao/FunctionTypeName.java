package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnTypeName;

/**
 * The type names a function's data type and parameters can have, stored by ordinal in the functions
 * tables with a CHECK on the range. A {@link ColumnTypeName} added after these (such as {@code
 * GEOMETRY}) is not stored there, because a step can't widen that CHECK on every database, so keep
 * these constants and their order fixed.
 */
public enum FunctionTypeName {
  BOOLEAN,
  BYTE,
  SHORT,
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  DATE,
  TIMESTAMP,
  TIMESTAMP_NTZ,
  STRING,
  BINARY,
  DECIMAL,
  INTERVAL,
  ARRAY,
  STRUCT,
  MAP,
  VARIANT,
  CHAR,
  NULL,
  USER_DEFINED_TYPE,
  TABLE_TYPE;

  public static FunctionTypeName from(ColumnTypeName typeName) {
    if (typeName == null) {
      return null;
    }
    try {
      return valueOf(typeName.name());
    } catch (IllegalArgumentException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Type " + typeName.getValue() + " is not supported in functions.");
    }
  }

  public ColumnTypeName toColumnTypeName() {
    return ColumnTypeName.valueOf(name());
  }
}
