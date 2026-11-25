ALTER TABLE uc_storage_cleanup_tasks
    DROP CONSTRAINT uc_storage_cleanup_tasks_resource_type_check,
    ADD CONSTRAINT uc_storage_cleanup_tasks_resource_type_check
        CHECK (resource_type IN ('TABLE', 'REGISTERED_TABLE', 'VOLUME', 'REGISTERED_MODEL', 'MODEL_VERSION', 'STAGING_TABLE'));
