ALTER TABLE uc_function_params ADD CONSTRAINT uc_function_params_type_name_range CHECK (type_name BETWEEN 0 AND 23);
