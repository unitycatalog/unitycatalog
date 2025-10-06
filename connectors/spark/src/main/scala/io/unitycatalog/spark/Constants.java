package io.unitycatalog.spark;

public class Constants {
    public Constants() {}

    public static final String UNITY_CATALOG_URI = "fs.unitycatalog.uri";
    public static final String UNITY_CATALOG_TOKEN = "fs.unitycatalog.token";

    // Keys for table based temporary credential requests
    public static final String UNITY_CATALOG_TABLE = "fs.unitycatalog.table";
    public static final String UNITY_CATALOG_TABLE_OPERATION = "fs.unitycatalog.table.operation";

    // Keys for path based temporary credential requests.
    public static final String UNITY_CATALOG_PATH = "fs.unitycatalog.path";
    public static final String UNITY_CATALOG_PATH_OPERATION = "fs.unitycatalog.path.operation";

    public static final String UNITY_CATALOG_CREDENTIALS_TYPE = "fs.unitycatalog.credentials.type";
    public static final String UNITY_CATALOG_TABLE_CREDENTIALS_TYPE = "table";
    public static final String UNITY_CATALOG_PATH_CREDENTIALS_TYPE = "path";
}
