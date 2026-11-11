ALTER TABLE uc_function_params ALTER COLUMN type_text TYPE text;
ALTER TABLE uc_function_params ALTER COLUMN comment TYPE varchar(65535);
ALTER TABLE uc_function_params ALTER COLUMN parameter_default TYPE varchar(65535);
ALTER TABLE uc_functions ALTER COLUMN full_data_type TYPE text;
ALTER TABLE uc_schemas ALTER COLUMN comment TYPE varchar(65535);
ALTER TABLE uc_volumes ALTER COLUMN comment TYPE varchar(65535);
