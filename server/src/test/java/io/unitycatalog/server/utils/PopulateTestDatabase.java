package io.unitycatalog.server.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.HibernateUtil;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.dao.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.*;

import static io.unitycatalog.server.utils.ColumnUtils.*;

/**
 * This is a utility class to populate the test database with some sample data.
 * All the quickstart examples in the documentation are based on this sample data.
 * This class is not part of the main application code and is only used for testing purposes.
 * To recreate the sample data, first delete the existing database (at /etc/db)
 * and then run main method of this class.
 * Any data artifacts which might be referred to by storage location of any table/volume
 * should be created before creating the table/volume entry in the database or can be a part of the same PR.
 */
public class PopulateTestDatabase {

    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("Populating test database...");

        System.setProperty("server.env", "dev");
        String catalogName = "unity";
        String schemaName = "default";

        CreateCatalog catalog1 = new CreateCatalog().name(catalogName).comment("Main catalog");
        CatalogRepository.getInstance().addCatalog(catalog1);

        CreateSchema schema1 = new CreateSchema().name(schemaName).catalogName(catalogName).comment("Default schema");
        SchemaRepository.getInstance().createSchema(schema1);

        SchemaInfo schemaInfo = SchemaRepository.getInstance().getSchema(catalogName + "." + schemaName);
        String schemaId = schemaInfo.getSchemaId();


        SessionFactory factory = HibernateUtil.getSessionFactory();

        // Create managed table
        ColumnInfoDAO idColumn = ColumnInfoDAO
                .builder()
                .name("id")
                .typeName(ColumnTypeName.INT.getValue())
                .comment("ID primary key")
                .ordinalPosition((short) 0)
                .build();
        addTypeTextAndJsonText(idColumn);

        ColumnInfoDAO nameColumn = ColumnInfoDAO
                .builder()
                .name("name")
                .typeName(ColumnTypeName.STRING.getValue())
                .comment("Name of the entity")
                .ordinalPosition((short) 1)
                .build();
        addTypeTextAndJsonText(nameColumn);

        ColumnInfoDAO marksColumn = ColumnInfoDAO
                .builder()
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

        System.setProperty("storageRoot", storageRoot);

        TableInfoDAO tableInfoDAO = TableInfoDAO
                .builder()
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

        tableInfoDAO.getColumns().forEach(columnInfoDAO -> columnInfoDAO.setTableId(tableInfoDAO));

        PropertyDAO p1 = PropertyDAO.builder().key("key1").value("value1").entityId(tableId).entityType("table").build();
        PropertyDAO p2 = PropertyDAO.builder().key("key2").value("value2").entityId(tableId).entityType("table").build();

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
        ColumnInfoDAO idColumnUniform = ColumnInfoDAO
                .builder()
                .name("id")
                .typeName(ColumnTypeName.INT.getValue())
                .comment("ID primary key")
                .ordinalPosition((short) 0)
                .build();
        addTypeTextAndJsonText(idColumnUniform);

        ColumnInfoDAO nameColumnUniform = ColumnInfoDAO
                .builder()
                .name("name")
                .typeName(ColumnTypeName.STRING.getValue())
                .comment("Name of the entity")
                .ordinalPosition((short) 1)
                .build();
        addTypeTextAndJsonText(nameColumnUniform);

        ColumnInfoDAO marksColumnUniform = ColumnInfoDAO
                .builder()
                .name("marks")
                .typeName(ColumnTypeName.INT.getValue())
                .comment("Marks of the entity")
                .ordinalPosition((short) 2)
                .nullable(true)
                .build();
        addTypeTextAndJsonText(marksColumnUniform);

        TableInfoDAO uniformTableInfoDAO = TableInfoDAO
                .builder()
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
                .uniformIcebergMetadataLocation(uniformTablePath + "/metadata/00002-5b7aa739-d074-4764-b49d-ad6c63419576.metadata.json")
                .build();
        uniformTableInfoDAO.getColumns().forEach(columnInfoDAO -> columnInfoDAO.setTableId(uniformTableInfoDAO));
        PropertyDAO p1uniform = PropertyDAO.builder().key("key1").value("value1").entityId(uniformTableId).entityType("table").build();
        PropertyDAO p2uniform = PropertyDAO.builder().key("key2").value("value2").entityId(uniformTableId).entityType("table").build();
        try (Session session = factory.openSession()) {
            Transaction tx = session.beginTransaction();
            session.persist(uniformTableInfoDAO);
            session.persist(p1uniform);
            session.persist(p2uniform);
            tx.commit();
        }

        // create external table
        ColumnInfoDAO idColumn1 = ColumnInfoDAO
                .builder()
                .name("as_int")
                .typeName(ColumnTypeName.INT.getValue())
                .comment("Int column")
                .ordinalPosition((short) 0)
                .build();
        addTypeTextAndJsonText(idColumn1);
        ColumnInfoDAO doubleColumn2 = ColumnInfoDAO
                .builder()
                .name("as_double")
                .typeName(ColumnTypeName.DOUBLE.getValue())
                .comment("Double column")
                .ordinalPosition((short) 1)
                .build();
        addTypeTextAndJsonText(doubleColumn2);

        String externalTableName = "numbers";
        String externalStorageRoot = "etc/data/external/";
        String externalTablePath = externalStorageRoot + catalogName + "/" + schemaName + "/tables/" + externalTableName;

        UUID externalTableId = UUID.randomUUID();

        System.setProperty("storageRoot", externalStorageRoot);

        TableInfoDAO externalTableInfoDAO = TableInfoDAO
                .builder()
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

        externalTableInfoDAO.getColumns().forEach(columnInfoDAO -> columnInfoDAO.setTableId(externalTableInfoDAO));

        PropertyDAO p11 = PropertyDAO.builder().key("key1").value("value1").entityId(externalTableId).entityType("table").build();
        PropertyDAO p21 = PropertyDAO.builder().key("key2").value("value2").entityId(externalTableId).entityType("table").build();

        try (Session session = factory.openSession()) {
            Transaction tx = session.beginTransaction();
            session.persist(externalTableInfoDAO);
            session.persist(p11);
            session.persist(p21);
            tx.commit();
        }

        System.out.println("Creating managed/external Volume...");
        VolumeInfoDAO managedVolume = VolumeInfoDAO.builder()
                .volumeType(VolumeType.MANAGED.getValue())
                .storageLocation("etc/data/managed/unity/default/volumes/txt_files")
                .name("txt_files")
                .createdAt(new Date())
                .updatedAt(new Date())
                .id(UUID.randomUUID())
                .schemaId(UUID.fromString(schemaId))
                .build();

        VolumeInfoDAO externalVolume = VolumeInfoDAO.builder()
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

        insertFunctionSampleData(catalogName, schemaName);
    }
    public static void insertFunctionSampleData(String catalog, String schema) {

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

        FunctionRepository functionRepository = FunctionRepository.getInstance();
        functionRepository.createFunction(new CreateFunctionRequest().functionInfo(sumFunction));

        functionRepository.createFunction(new CreateFunctionRequest().functionInfo(stringLowercaseFunction));

    }

}