![image info](./../uc-logo-horiz.png)

# Unity Catalog: Open, Multi-modal Catalog for Data & AI
Unity Catalog is the most open and interoperable catalog for data and AI
- **Universal interface supports any format, engine, and asset**
    - Multi-format table support - Delta, Iceberg as UniForm, Parquet, CSV, etc.
    - Beyond tables - Unstructured data (Volumes) and AI assets (ML models, Gen AI tools)
    - Plugin support - extensible to Iceberg REST Catalog and HMS interface for client compatibility,
      plus additional plugins (e.g., integration with a new AI framework)
    - Interoperates with the [Delta Sharing open protocol](https://delta.io/sharing/) for sharing tabular and non-tabular assets cross-domain
- **Fully Open** - OpenAPI spec and OSS implementation (Apache 2.0 license)
- **Unified governance** for data and AI - Asset-level access control is enforced through
  temporary credential vending via REST APIs

![image info](./../uc.png)

This documentation contains the following
- [Tutorial for Tables, Volumes, Functions, etc.](./tutorial.md)
- [UC Server Documentation](./server.md)
- [UC CLI Documentation](./cli.md)
- [UC Rest API specification](../api/)
