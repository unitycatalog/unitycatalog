# Deployment

This guide outlines how to deploy the Unity Catalog server.

## Deploying using tarball

### Prerequisites

- To generate the tarball, run the following command in the source code:
  ```sh
  build/sbt createTarball
  ```

### Unpacking the tarball

- The tarball generated in the `target` directory can be unpacked using the following command:
  ```sh
  tar -xvf unitycatalog-<version>.tar.gz
  ```
- Unpacking the tarball will create the following directory structure:
  ```
  unitycatalog-<version>
  ├── bin
  │   ├── start-uc-server
  │   └── uc
  ├── etc
  │   ├── conf
  │   ├── data
  │   ├── db
  │   └── logs
  └── jars
  ```
  - The `bin` directory contains the scripts that you can use to start the UC server and run the CLI.
  - The `etc` directory contains the configuration, data, database, and logs directories.
  - The `jars` directory contains the jar files required to run the UC server.

### Configuring the UC server

- The UC server can be configured by modifying the files in `etc/conf/`. This includes properties related to logging, server environment and the s3 configuration.
- The `etc/data/` directory contains the data files that are used by the UC server. This includes the tables and volumes that are created.
- The `etc/db/` directory contains the backend database that is used by the UC server.

### Configuring the database

- The backend database can be configured by modifying the `etc/conf/server.property` file.
- Choose the database type (`db.type`) to configure the backend database.
- For H2 Database:
  - Setting the server environment to `dev` will use the local file system for storing the backend database whereas `test` will spin up an in-memory database.
- For MySQL Database:
  - You need to provide the connection details to connect to your MySQL server. The following is how to quickstart a MySQL connection: 

#### Prerequisites
- install docker

#### Start MySQL server.

- In a terminal, navigate to the cloned repository root directory
- Modify `etc/db/docker-compose.yml` to configure MySQL server. Then start MySQL using Docker:
```sh
docker-compose -f etc/db/docker-compose.yml up -d
```

- Modify the `etc/conf/server.properties` file with your MySQL connection details:
```properties
db.type=mysql
jdbc.url=jdbc:mysql://localhost:3306/ucdb
jdbc.user=uc_default_user
jdbc.password=uc_default_password
```