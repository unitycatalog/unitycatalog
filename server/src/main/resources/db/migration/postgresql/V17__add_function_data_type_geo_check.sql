ALTER TABLE uc_functions ADD CONSTRAINT uc_functions_data_type_range CHECK (data_type BETWEEN 0 AND 23);
