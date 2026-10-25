-- Changes a table of this release by hand, so the schema check after the steps fails.
ALTER TABLE uc_function_params ADD COLUMN test_note VARCHAR(64);
