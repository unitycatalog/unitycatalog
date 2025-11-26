# Unity Catalog Trino Integration

## Setting up REST Catalog with Trino

After setting up Trino, REST Catalog connection can be setup by adding a `etc/catalog/iceberg.properties` file to configure Trino to use Unity Catalog's Iceberg REST API Catalog endpoint.

```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=not_used
```

Once your properties file is configured, you can run the Trino CLI and issue a SQL query against the Delta UniForm table:

```sql
SELECT * FROM iceberg."unity.default".marksheet_uniform
```
