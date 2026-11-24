package io.unitycatalog.server.migrate;

/** Drops the old 0..21 ordinal CHECK on {@code uc_functions.data_type} after V17 adds 0..23. */
public final class V18__drop_old_function_data_type_check extends DropRangeCheckMigration {
  public V18__drop_old_function_data_type_check() {
    super("uc_functions", "data_type", 0, 21);
  }
}
