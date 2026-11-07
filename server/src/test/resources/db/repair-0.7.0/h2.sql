-- The repair SQL of docs/server/upgrades.md for H2; keep the two the same.
-- <check of uc_functions.data_type>: SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_name = 'UC_FUNCTIONS' AND cc.check_clause = '"DATA_TYPE" BETWEEN 0 AND 20';
-- <check of uc_function_params.type_name>: SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_name = 'UC_FUNCTION_PARAMS' AND cc.check_clause = '"TYPE_NAME" BETWEEN 0 AND 20';
ALTER TABLE uc_functions DROP CONSTRAINT "<check of uc_functions.data_type>";
ALTER TABLE uc_functions ADD CHECK (data_type BETWEEN 0 AND 21);
ALTER TABLE uc_function_params DROP CONSTRAINT "<check of uc_function_params.type_name>";
ALTER TABLE uc_function_params ADD CHECK (type_name BETWEEN 0 AND 21);
