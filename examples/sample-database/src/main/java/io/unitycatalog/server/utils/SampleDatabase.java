package io.unitycatalog.server.utils;

import static io.unitycatalog.server.utils.ColumnUtils.addTypeTextAndJsonText;
import static io.unitycatalog.server.utils.ColumnUtils.getTypeJson;
import static io.unitycatalog.server.utils.ColumnUtils.getTypeText;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.migrate.SchemaMigrator;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.CreateFunction;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.FunctionParameterInfo;
import io.unitycatalog.server.model.FunctionParameterInfos;
import io.unitycatalog.server.model.FunctionParameterMode;
import io.unitycatalog.server.model.FunctionParameterType;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 * Builds the sample H2 database that the quickstart examples in the documentation use: the {@code
 * unity} catalog with its {@code default} schema, tables, volumes and functions. The tables and
 * volumes point at the files under {@code etc/data}; add any file that a new table or volume points
 * at in the same change. It is built, not checked in: {@code build/sbt buildSampleDatabase} builds
 * {@code etc/db/h2db.mv.db}, {@code bin/start-uc-server} does so in a source checkout where the
 * file is missing, and the tarball and the Docker image ship one built by their build.
 */
public class SampleDatabase {

  private static final String H2_FILE_SUFFIX = ".mv.db";

  /** Builds the sample database at the path given as the only argument. */
  public static void main(String[] args) {
    build(Path.of(args[0]));
  }

  /**
   * Builds the sample database in the H2 file {@code h2File} (named {@code <name>.mv.db}): sets up
   * this build's schema with the schema steps, as {@code bin/start-uc-server --upgrade-schema}
   * does, then writes the sample data through the repositories. The file appears only when the
   * build is done, and the build fails if it already exists.
   */
  public static void build(Path h2File) {
    System.out.println("Building the sample database " + h2File + "...");
    // Built beside the target under a name of its own, so builds at the same time don't share a
    // file, and moved there only when done: a failed build leaves no file for bin/start-uc-server
    // to take for the sample database.
    Path building = null;
    try {
      building =
          Files.createTempFile(h2File.toAbsolutePath().getParent(), "building-", H2_FILE_SUFFIX);
      buildInto(building);
      Files.move(building, h2File);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      try {
        if (building != null) {
          Files.deleteIfExists(building);
        }
      } catch (IOException e) {
        // The build's own failure matters more.
      }
    }
  }

  private static void buildInto(Path h2File) {
    String database = h2File.toAbsolutePath().toString();
    Properties hibernateProperties = new Properties();
    hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    // The URL options of etc/conf/hibernate.properties, as H2 stores DB_CLOSE_DELAY in the file.
    hibernateProperties.setProperty(
        "hibernate.connection.url",
        "jdbc:h2:file:"
            + database.substring(0, database.length() - H2_FILE_SUFFIX.length())
            + ";DB_CLOSE_ON_EXIT=FALSE;DB_CLOSE_DELAY=0");
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "none");
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "dev");
    ServerProperties serverProperties = new ServerProperties(properties);
    // The repositories read the caller from the request's context: none here, so no owner.
    try (HibernateConfigurator hibernateConfigurator =
            new HibernateConfigurator(hibernateProperties);
        SafeCloseable request =
            ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
      new SchemaMigrator(hibernateConfigurator.getDataSource()).upgradeSchema();
      writeSampleData(
          hibernateConfigurator.getSessionFactory(),
          new Repositories(hibernateConfigurator.getSessionFactory(), serverProperties));
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeSampleData(SessionFactory factory, Repositories repositories)
      throws JsonProcessingException {
    CatalogRepository catalogRepository = repositories.getCatalogRepository();
    SchemaRepository schemaRepository = repositories.getSchemaRepository();

    String catalogName = "unity";
    String schemaName = "default";

    CreateCatalog catalog1 = new CreateCatalog().name(catalogName).comment("Main catalog");
    catalogRepository.addCatalog(catalog1);

    CreateSchema schema1 =
        new CreateSchema().name(schemaName).catalogName(catalogName).comment("Default schema");
    schemaRepository.createSchema(schema1);

    SchemaInfo schemaInfo = schemaRepository.getSchema(catalogName + "." + schemaName);
    String schemaId = schemaInfo.getSchemaId();

    // Create managed table
    ColumnInfoDAO idColumn =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("id")
            .typeName(ColumnTypeName.INT.getValue())
            .comment("ID primary key")
            .ordinalPosition((short) 0)
            .build();
    addTypeTextAndJsonText(idColumn);

    ColumnInfoDAO nameColumn =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("name")
            .typeName(ColumnTypeName.STRING.getValue())
            .comment("Name of the entity")
            .ordinalPosition((short) 1)
            .build();
    addTypeTextAndJsonText(nameColumn);

    ColumnInfoDAO marksColumn =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("marks")
            .typeName(ColumnTypeName.INT.getValue())
            .comment("Marks of the entity")
            .ordinalPosition((short) 2)
            .nullable(true)
            .build();
    addTypeTextAndJsonText(marksColumn);

    UUID tableId = UUID.randomUUID();

    String tableName = "marksheet";
    String storageRoot = "etc/data/managed/";
    String tablePath = storageRoot + catalogName + "/" + schemaName + "/tables/" + tableName;

    TableInfoDAO tableInfoDAO =
        TableInfoDAO.builder()
            .id(tableId)
            .name(tableName)
            .schemaId(UUID.fromString(schemaId))
            .comment("Managed table")
            .columns(List.of(idColumn, nameColumn, marksColumn))
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .type(TableType.MANAGED.getValue())
            .createdAt(new Date())
            .updatedAt(new Date())
            .url(tablePath)
            .build();

    tableInfoDAO.getColumns().forEach(columnInfoDAO -> columnInfoDAO.setTable(tableInfoDAO));

    PropertyDAO p1 =
        PropertyDAO.builder()
            .key("key1")
            .value("value1")
            .entityId(tableId)
            .entityType("table")
            .build();
    PropertyDAO p2 =
        PropertyDAO.builder()
            .key("key2")
            .value("value2")
            .entityId(tableId)
            .entityType("table")
            .build();

    try (Session session = factory.openSession()) {
      Transaction tx = session.beginTransaction();
      session.persist(tableInfoDAO);
      session.persist(p1);
      session.persist(p2);
      tx.commit();
    }

    // create uniform table
    String uniformTableName = "marksheet_uniform";
    String uniformTablePath = "file:///tmp/" + uniformTableName;
    UUID uniformTableId = UUID.randomUUID();
    ColumnInfoDAO idColumnUniform =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("id")
            .typeName(ColumnTypeName.INT.getValue())
            .comment("ID primary key")
            .ordinalPosition((short) 0)
            .build();
    addTypeTextAndJsonText(idColumnUniform);

    ColumnInfoDAO nameColumnUniform =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("name")
            .typeName(ColumnTypeName.STRING.getValue())
            .comment("Name of the entity")
            .ordinalPosition((short) 1)
            .build();
    addTypeTextAndJsonText(nameColumnUniform);

    ColumnInfoDAO marksColumnUniform =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("marks")
            .typeName(ColumnTypeName.INT.getValue())
            .comment("Marks of the entity")
            .ordinalPosition((short) 2)
            .nullable(true)
            .build();
    addTypeTextAndJsonText(marksColumnUniform);

    TableInfoDAO uniformTableInfoDAO =
        TableInfoDAO.builder()
            .id(uniformTableId)
            .name(tableName + "_uniform")
            .schemaId(UUID.fromString(schemaId))
            .comment("Uniform table")
            .columns(List.of(idColumnUniform, nameColumnUniform, marksColumnUniform))
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .type(TableType.EXTERNAL.getValue())
            .createdAt(new Date())
            .updatedAt(new Date())
            .url(uniformTablePath)
            .icebergMetadataLocation(
                uniformTablePath
                    + "/metadata/00002-5b7aa739-d074-4764-b49d-ad6c63419576.metadata.json")
            .build();
    uniformTableInfoDAO
        .getColumns()
        .forEach(columnInfoDAO -> columnInfoDAO.setTable(uniformTableInfoDAO));
    PropertyDAO p1uniform =
        PropertyDAO.builder()
            .key("key1")
            .value("value1")
            .entityId(uniformTableId)
            .entityType("table")
            .build();
    PropertyDAO p2uniform =
        PropertyDAO.builder()
            .key("key2")
            .value("value2")
            .entityId(uniformTableId)
            .entityType("table")
            .build();
    try (Session session = factory.openSession()) {
      Transaction tx = session.beginTransaction();
      session.persist(uniformTableInfoDAO);
      session.persist(p1uniform);
      session.persist(p2uniform);
      tx.commit();
    }

    // create external table
    ColumnInfoDAO idColumn1 =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("as_int")
            .typeName(ColumnTypeName.INT.getValue())
            .comment("Int column")
            .ordinalPosition((short) 0)
            .build();
    addTypeTextAndJsonText(idColumn1);
    ColumnInfoDAO doubleColumn2 =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("as_double")
            .typeName(ColumnTypeName.DOUBLE.getValue())
            .comment("Double column")
            .ordinalPosition((short) 1)
            .build();
    addTypeTextAndJsonText(doubleColumn2);

    String externalTableName = "numbers";
    String externalStorageRoot = "etc/data/external/";
    String externalTablePath =
        externalStorageRoot + catalogName + "/" + schemaName + "/tables/" + externalTableName;

    UUID externalTableId = UUID.randomUUID();

    TableInfoDAO externalTableInfoDAO =
        TableInfoDAO.builder()
            .id(externalTableId)
            .name(externalTableName)
            .schemaId(UUID.fromString(schemaId))
            .comment("External table")
            .columns(List.of(idColumn1, doubleColumn2))
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .type(TableType.EXTERNAL.getValue())
            .createdAt(new Date())
            .updatedAt(new Date())
            .url(externalTablePath)
            .build();

    externalTableInfoDAO
        .getColumns()
        .forEach(columnInfoDAO -> columnInfoDAO.setTable(externalTableInfoDAO));

    PropertyDAO p11 =
        PropertyDAO.builder()
            .key("key1")
            .value("value1")
            .entityId(externalTableId)
            .entityType("table")
            .build();
    PropertyDAO p21 =
        PropertyDAO.builder()
            .key("key2")
            .value("value2")
            .entityId(externalTableId)
            .entityType("table")
            .build();

    try (Session session = factory.openSession()) {
      Transaction tx = session.beginTransaction();
      session.persist(externalTableInfoDAO);
      session.persist(p11);
      session.persist(p21);
      tx.commit();
    }

    // Create external partitioned table.
    // This table represents an example of how Unity handles partitioned tables.
    // The table contains three columns:
    // - first_name
    // - age
    // - country (partition column)
    // All the data in this table are fake and were generated by tool Faker.
    // Partition column has three unique values / partitions.
    // Data is stored in DELTA format.
    System.out.println("Create external partitioned table...");

    // Add columns
    ColumnInfoDAO firstName =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("first_name")
            .typeName(ColumnTypeName.STRING.getValue())
            .comment("string column")
            .ordinalPosition((short) 0)
            .build();
    addTypeTextAndJsonText(firstName);

    ColumnInfoDAO age =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("age")
            .typeName(ColumnTypeName.LONG.getValue())
            .comment("long column")
            .ordinalPosition((short) 1)
            .build();
    addTypeTextAndJsonText(age);

    ColumnInfoDAO country =
        ColumnInfoDAO.builder()
            .id(UUID.randomUUID())
            .name("country")
            .typeName(ColumnTypeName.STRING.getValue())
            .comment("partition column")
            .ordinalPosition((short) 2)
            .partitionIndex((short) 0)
            .build();
    addTypeTextAndJsonText(country);

    // Create table
    String partitionedTableName = "user_countries";
    String partitionedStorageRoot = "etc/data/external/";
    String partitionedTablePath =
        partitionedStorageRoot + catalogName + "/" + schemaName + "/tables/" + partitionedTableName;
    UUID partitionedTableId = UUID.randomUUID();

    TableInfoDAO partitionedTableInfoDAO =
        TableInfoDAO.builder()
            .id(partitionedTableId)
            .name(partitionedTableName)
            .schemaId(UUID.fromString(schemaId))
            .comment("Partitioned table")
            .columns(List.of(firstName, age, country))
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .type(TableType.EXTERNAL.getValue())
            .createdAt(new Date())
            .updatedAt(new Date())
            .url(partitionedTablePath)
            .build();

    partitionedTableInfoDAO
        .getColumns()
        .forEach(columnInfoDAO -> columnInfoDAO.setTable(partitionedTableInfoDAO));

    try (Session session = factory.openSession()) {
      Transaction trx = session.beginTransaction();
      session.persist(partitionedTableInfoDAO);
      trx.commit();
    }

    System.out.println("Creating managed/external Volume...");
    VolumeInfoDAO managedVolume =
        VolumeInfoDAO.builder()
            .volumeType(VolumeType.MANAGED.getValue())
            .storageLocation("etc/data/managed/unity/default/volumes/txt_files")
            .name("txt_files")
            .createdAt(new Date())
            .updatedAt(new Date())
            .id(UUID.randomUUID())
            .schemaId(UUID.fromString(schemaId))
            .build();

    VolumeInfoDAO externalVolume =
        VolumeInfoDAO.builder()
            .volumeType(VolumeType.EXTERNAL.getValue())
            .storageLocation("etc/data/external/unity/default/volumes/json_files")
            .name("json_files")
            .createdAt(new Date())
            .updatedAt(new Date())
            .id(UUID.randomUUID())
            .schemaId(UUID.fromString(schemaId))
            .build();

    try (Session session = factory.openSession()) {
      session.beginTransaction();
      session.persist(managedVolume);
      session.persist(externalVolume);
      session.getTransaction().commit();
    }

    insertFunctionSampleData(catalogName, schemaName, repositories);
  }

  private static void insertFunctionSampleData(
      String catalog, String schema, Repositories repositories) {

    // Create function objects
    CreateFunction sumFunction = new CreateFunction();
    sumFunction.setName("sum");
    sumFunction.setCatalogName(catalog);
    sumFunction.setSchemaName(schema);
    sumFunction.setComment("Adds two numbers.");
    sumFunction.setDataType(ColumnTypeName.INT);
    sumFunction.setFullDataType(ColumnTypeName.INT.getValue());
    sumFunction.setExternalLanguage("python");
    sumFunction.setIsDeterministic(true);
    sumFunction.setIsNullCall(false);
    sumFunction.setParameterStyle(CreateFunction.ParameterStyleEnum.S);
    sumFunction.setRoutineBody(CreateFunction.RoutineBodyEnum.EXTERNAL);
    sumFunction.setRoutineDefinition("t = x + y + z\\nreturn t");
    sumFunction.setSqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL);
    sumFunction.setSecurityType(CreateFunction.SecurityTypeEnum.DEFINER);
    sumFunction.setSpecificName("sum");

    CreateFunction stringLowercaseFunction = new CreateFunction();
    stringLowercaseFunction.setName("lowercase");
    stringLowercaseFunction.setCatalogName(catalog);
    stringLowercaseFunction.setSchemaName(schema);
    stringLowercaseFunction.setComment("Converts a string to lowercase.");
    stringLowercaseFunction.setDataType(ColumnTypeName.STRING);
    stringLowercaseFunction.setFullDataType(ColumnTypeName.STRING.getValue());
    stringLowercaseFunction.setExternalLanguage("python");
    stringLowercaseFunction.setIsDeterministic(true);
    stringLowercaseFunction.setIsNullCall(false);
    stringLowercaseFunction.setParameterStyle(CreateFunction.ParameterStyleEnum.S);
    stringLowercaseFunction.setRoutineBody(CreateFunction.RoutineBodyEnum.EXTERNAL);
    stringLowercaseFunction.setRoutineDefinition("g = s.lower()\\nreturn g");
    stringLowercaseFunction.setSqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL);
    stringLowercaseFunction.setSecurityType(CreateFunction.SecurityTypeEnum.DEFINER);
    stringLowercaseFunction.setSpecificName("lowercase");

    // Create parameter objects for sum function

    FunctionParameterInfo sumParam1 = new FunctionParameterInfo();
    sumParam1.setName("x");
    sumParam1.setTypeText(getTypeText(ColumnTypeName.INT));
    sumParam1.setTypeJson(getTypeJson(ColumnTypeName.INT, "x", false, null));
    sumParam1.setTypeName(ColumnTypeName.INT);
    sumParam1.setPosition(0);
    sumParam1.setParameterMode(FunctionParameterMode.IN);
    sumParam1.setParameterType(FunctionParameterType.PARAM);

    FunctionParameterInfo sumParam2 = new FunctionParameterInfo();
    sumParam2.setName("y");
    sumParam2.setTypeText(getTypeText(ColumnTypeName.INT));
    sumParam2.setTypeJson(getTypeJson(ColumnTypeName.INT, "y", false, null));
    sumParam2.setTypeName(ColumnTypeName.INT);
    sumParam2.setPosition(1);
    sumParam2.setParameterMode(FunctionParameterMode.IN);
    sumParam2.setParameterType(FunctionParameterType.PARAM);

    FunctionParameterInfo sumParam3 = new FunctionParameterInfo();
    sumParam3.setName("z");
    sumParam3.setTypeText(getTypeText(ColumnTypeName.INT));
    sumParam3.setTypeJson(getTypeJson(ColumnTypeName.INT, "z", false, null));
    sumParam3.setTypeName(ColumnTypeName.INT);
    sumParam3.setPosition(2);
    sumParam3.setParameterMode(FunctionParameterMode.IN);
    sumParam3.setParameterType(FunctionParameterType.PARAM);

    // Create parameter objects for lowercase function
    FunctionParameterInfo lowercaseParam = new FunctionParameterInfo();
    lowercaseParam.setName("s");
    lowercaseParam.setTypeText(getTypeText(ColumnTypeName.STRING));
    lowercaseParam.setTypeJson(getTypeJson(ColumnTypeName.STRING, "s", false, null));
    lowercaseParam.setTypeName(ColumnTypeName.STRING);
    lowercaseParam.setPosition(0);
    lowercaseParam.setParameterMode(FunctionParameterMode.IN);
    lowercaseParam.setParameterType(FunctionParameterType.PARAM);

    FunctionParameterInfos functionParameterInfos = new FunctionParameterInfos();
    functionParameterInfos.setParameters(List.of(sumParam1, sumParam2, sumParam3));
    sumFunction.setInputParams(functionParameterInfos);

    FunctionParameterInfos stringLowercaseFunctionParameterInfos = new FunctionParameterInfos();
    stringLowercaseFunctionParameterInfos.setParameters(List.of(lowercaseParam));
    stringLowercaseFunction.setInputParams(stringLowercaseFunctionParameterInfos);

    FunctionRepository functionRepository = repositories.getFunctionRepository();
    functionRepository.createFunction(new CreateFunctionRequest().functionInfo(sumFunction));

    functionRepository.createFunction(
        new CreateFunctionRequest().functionInfo(stringLowercaseFunction));
  }
}
