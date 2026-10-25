-- Text in Unity Catalog compares case-sensitively, like storage paths and like PostgreSQL and H2.
-- A database created with MySQL's default collation ignores case, so this step converts it and
-- every table that holds Unity Catalog's text to utf8mb4_0900_bin; tables created later take the
-- database's collation. casbin_rule (the authorization rules, created by Casbin's adapter) is
-- converted too when it exists, since the adapter finds the rules to delete by comparing text.
--
-- One step with many statements, unlike the other steps: converting a table that is already
-- utf8mb4_0900_bin changes nothing, so the step is safe on databases created with that collation.

SET FOREIGN_KEY_CHECKS = 0;

ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

-- A 0.7.0 server on a latin1 database created this index; in utf8mb4 its key is too long, and the
-- baseline never has it.
SET @drop_staging_location_index = IF(
    EXISTS (SELECT 1 FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'uc_staging_tables'
              AND INDEX_NAME = 'idx_staging_location'),
    'DROP INDEX idx_staging_location ON uc_staging_tables',
    'DO 0');
PREPARE drop_staging_location_index FROM @drop_staging_location_index;
EXECUTE drop_staging_location_index;
DEALLOCATE PREPARE drop_staging_location_index;

ALTER TABLE uc_catalogs CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_columns CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_credentials CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_delta_commits CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_dependencies CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_external_locations CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_function_params CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_functions CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_metastore CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_model_versions CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_properties CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_registered_models CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_schemas CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_staging_tables CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_storage_cleanup_tasks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_tables CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_users CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE uc_volumes CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

SET @convert_casbin_rule = IF(
    EXISTS (SELECT 1 FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'casbin_rule'),
    'ALTER TABLE casbin_rule CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin',
    'DO 0');
PREPARE convert_casbin_rule FROM @convert_casbin_rule;
EXECUTE convert_casbin_rule;
DEALLOCATE PREPARE convert_casbin_rule;

SET FOREIGN_KEY_CHECKS = 1;
