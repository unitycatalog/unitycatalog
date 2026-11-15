ALTER TABLE uc_users ADD CONSTRAINT uc_users_email_key UNIQUE (email);
ALTER TABLE uc_model_versions ADD COLUMN created_by_id binary(16);
ALTER TABLE uc_staging_tables ADD COLUMN created_by_id binary(16);
