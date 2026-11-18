ALTER TABLE uc_tables ADD CONSTRAINT uk_uc_tables_schema_name UNIQUE (schema_id, name);
