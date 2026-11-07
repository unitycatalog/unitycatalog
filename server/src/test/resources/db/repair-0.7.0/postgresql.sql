-- The repair SQL of docs/server/upgrades.md for PostgreSQL; keep the two the same.
-- <check of uc_functions.data_type>: SELECT conname FROM pg_constraint WHERE conrelid = 'uc_functions'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) = 'CHECK (((data_type >= 0) AND (data_type <= 20)))';
-- <check of uc_function_params.type_name>: SELECT conname FROM pg_constraint WHERE conrelid = 'uc_function_params'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) = 'CHECK (((type_name >= 0) AND (type_name <= 20)))';
ALTER TABLE uc_functions DROP CONSTRAINT "<check of uc_functions.data_type>", ADD CHECK (data_type BETWEEN 0 AND 21);
ALTER TABLE uc_function_params DROP CONSTRAINT "<check of uc_function_params.type_name>", ADD CHECK (type_name BETWEEN 0 AND 21);
