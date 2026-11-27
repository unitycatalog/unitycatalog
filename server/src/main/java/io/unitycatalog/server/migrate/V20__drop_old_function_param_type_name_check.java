package io.unitycatalog.server.migrate;

/**
 * Drops the old 0..21 ordinal CHECK on {@code uc_function_params.type_name} after V19 adds 0..23.
 */
public final class V20__drop_old_function_param_type_name_check extends DropRangeCheckMigration {
  public V20__drop_old_function_param_type_name_check() {
    super("uc_function_params", "type_name", 0, 21);
  }
}
