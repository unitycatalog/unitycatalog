ALTER TABLE uc_function_params ALTER COLUMN type_text CLOB NOT NULL;
ALTER TABLE uc_function_params ALTER COLUMN comment VARCHAR(65535);
ALTER TABLE uc_function_params ALTER COLUMN parameter_default VARCHAR(65535);
ALTER TABLE uc_functions ALTER COLUMN full_data_type CLOB;
ALTER TABLE uc_schemas ALTER COLUMN comment VARCHAR(65535);
ALTER TABLE uc_volumes ALTER COLUMN comment VARCHAR(65535);
