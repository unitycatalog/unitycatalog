package io.unitycatalog.spark;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * A wrapper around Spark's Table that adds V1_BATCH_WRITE capability.
 *
 * <p>In Spark 4.0, tables must explicitly declare the V1_BATCH_WRITE capability to support INSERT
 * operations. The default V1Table implementation returns an empty capability set, which causes
 * INSERT statements to fail with "unsupported append in batch mode" errors.
 *
 * <p>This wrapper delegates all methods to the underlying table while adding the V1_BATCH_WRITE
 * capability to enable write operations.
 */
public class UCTable implements Table {
  private final Table delegate;
  private final Set<TableCapability> capabilities;

  public UCTable(Table delegate) {
    this.delegate = delegate;
    // Add V1_BATCH_WRITE capability to enable INSERT operations
    this.capabilities = java.util.EnumSet.of(TableCapability.V1_BATCH_WRITE);
    // Preserve any capabilities from the delegate table
    this.capabilities.addAll(delegate.capabilities());
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public StructType schema() {
    return delegate.schema();
  }

  @Override
  public Transform[] partitioning() {
    return delegate.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return capabilities;
  }
}
