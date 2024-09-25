package io.unitycatalog.server.persist;

import java.util.UUID;

/** The metastore is an object/resource to allow granting catalog-wide privileges. */
public class MetastoreRepository {

  private static final UUID METASTORE_ID = UUID.fromString("ca7a1095-537c-4f9c-a136-5ca1ab1ec0de");

  private static final MetastoreRepository INSTANCE = new MetastoreRepository();

  public static MetastoreRepository getInstance() {
    return INSTANCE;
  }

  public UUID getMetastoreId() {
    return METASTORE_ID;
  }
}
