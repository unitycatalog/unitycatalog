# Object Hierarchy

- **Metastore**  
  - **Catalog**  
    - **Schema**  
      - **Table**  
      - **Volume**  
      - **Function**

---

# Privileges by Object

## Metastore
- `CREATE CATALOG`

## Catalog
- **Creation & Management**  
  - `CREATE FUNCTION`  
  - `CREATE SCHEMA`  
  - `CREATE TABLE`  
  - `CREATE MODEL`  
  - `CREATE VOLUME`  
  - `READ VOLUME`
- **Usage**  
  - `USE CATALOG`  
  - `USE SCHEMA`
- **Data Operations**  
  - `SELECT`  
  - `MODIFY`  
  - `EXECUTE`

## Schema
- **Creation & Management**  
  - `CREATE FUNCTION`  
  - `CREATE MODEL`  
  - `CREATE TABLE`  
  - `CREATE VOLUME`  
  - `READ VOLUME`
- **Usage**  
  - `USE SCHEMA`
- **Data Operations**  
  - `SELECT`  
  - `MODIFY`  
  - `EXECUTE`

## Table
- `SELECT`  
- `MODIFY`

## Volume
- `READ VOLUME`

## Function
- `EXECUTE`
