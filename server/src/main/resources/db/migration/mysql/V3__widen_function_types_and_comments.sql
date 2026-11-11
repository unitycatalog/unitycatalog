ALTER TABLE uc_function_params MODIFY COLUMN type_text mediumtext NOT NULL;
ALTER TABLE uc_function_params MODIFY COLUMN comment text NULL;
ALTER TABLE uc_function_params MODIFY COLUMN parameter_default text NULL;
ALTER TABLE uc_functions MODIFY COLUMN full_data_type mediumtext NULL;
ALTER TABLE uc_schemas MODIFY COLUMN comment text NULL;
ALTER TABLE uc_volumes MODIFY COLUMN comment text NULL;
