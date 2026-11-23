ALTER TABLE uc_storage_cleanup_tasks MODIFY COLUMN resource_type enum ('MODEL_VERSION', 'REGISTERED_MODEL', 'REGISTERED_TABLE', 'STAGING_TABLE', 'TABLE', 'VOLUME') not null;
