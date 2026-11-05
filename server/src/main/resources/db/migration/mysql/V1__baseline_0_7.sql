    -- The schema of release 0.7.0. Every statement skips what is already there.

    create table if not exists uc_catalogs (
        created_at datetime(6),
        updated_at datetime(6),
        id binary(16) not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        storage_root varchar(255),
        updated_by varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table if not exists uc_tables (
        column_count integer,
        created_at datetime(6),
        uniform_iceberg_converted_delta_timestamp datetime(6),
        uniform_iceberg_converted_delta_version bigint,
        updated_at datetime(6),
        id binary(16) not null,
        schema_id binary(16),
        url varchar(2048),
        created_by varchar(255),
        data_source_format varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        type varchar(255),
        updated_by varchar(255),
        comment text,
        uniform_iceberg_metadata_location text,
        view_definition mediumtext,
        primary key (id),
        index idx_name (name)
    ) engine=InnoDB;

    create table if not exists uc_columns (
        nullable bit not null,
        ordinal_position smallint not null,
        partition_index smallint,
        type_precision integer,
        type_scale integer,
        id binary(16) not null,
        table_id binary(16) not null,
        type_name varchar(32) not null,
        name varchar(255) not null,
        type_interval_type varchar(255),
        comment text,
        type_json mediumtext not null,
        type_text mediumtext not null,
        primary key (id),
        constraint UKacw3qyt0a4xgqxpuo0i2ee12x unique (table_id, ordinal_position, name),
        constraint FK1xslfe25li3uopodup6mylm3l foreign key (table_id) references uc_tables (id)
    ) engine=InnoDB;

    create table if not exists uc_credentials (
        purpose tinyint not null check (purpose between 0 and 0),
        created_at datetime(6) not null,
        updated_at datetime(6),
        id binary(16) not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        credential tinytext not null,
        credential_type enum ('AWS_IAM_ROLE') not null,
        primary key (id)
    ) engine=InnoDB;

    create table if not exists uc_delta_commits (
        is_backfilled_latest_commit bit not null,
        commit_file_modification_timestamp datetime(6) not null,
        commit_filesize bigint not null,
        commit_timestamp datetime(6) not null,
        commit_version bigint not null,
        id binary(16) not null,
        table_id binary(16) not null,
        commit_filename varchar(255) not null,
        primary key (id),
        constraint UKj7f8drl3byq8f2999frq9w2cv unique (table_id, commit_version)
    ) engine=InnoDB;

    create table if not exists uc_dependencies (
        dependent_id binary(16) not null,
        id binary(16) not null,
        dependency_catalog varchar(255),
        dependency_name varchar(255),
        dependency_schema varchar(255),
        dependency_type enum ('FUNCTION','TABLE') not null,
        dependent_type enum ('TABLE') not null,
        primary key (id),
        index idx_dependent (dependent_type, dependent_id)
    ) engine=InnoDB;

    create table if not exists uc_external_locations (
        created_at datetime(6),
        updated_at datetime(6),
        credential_id binary(16) not null,
        id binary(16) not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        url varchar(255) not null,
        primary key (id),
        index idx_url (url),
        index idx_credential_id (credential_id)
    ) engine=InnoDB;

    create table if not exists uc_functions (
        data_type tinyint check (data_type between 0 and 21),
        is_deterministic bit,
        is_null_call bit,
        parameter_style tinyint check (parameter_style between 0 and 0),
        routine_body tinyint check (routine_body between 0 and 1),
        security_type tinyint check (security_type between 0 and 0),
        sql_data_access tinyint check (sql_data_access between 0 and 2),
        created_at bigint,
        updated_at bigint,
        id binary(16) not null,
        schema_id binary(16),
        created_by varchar(255),
        external_language varchar(255),
        full_data_type varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        specific_name varchar(255),
        updated_by varchar(255),
        comment text,
        routine_definition mediumtext,
        primary key (id)
    ) engine=InnoDB;

    create table if not exists uc_function_params (
        input_or_return tinyint not null check (input_or_return between 0 and 1),
        parameter_mode tinyint check (parameter_mode between 0 and 0),
        parameter_type tinyint check (parameter_type between 0 and 1),
        position integer,
        type_name tinyint not null check (type_name between 0 and 21),
        type_precision integer,
        type_scale integer,
        function_id binary(16),
        id binary(16) not null,
        comment varchar(255),
        name varchar(255) not null,
        parameter_default varchar(255),
        type_interval_type varchar(255),
        type_json varchar(255),
        type_text varchar(255) not null,
        primary key (id),
        constraint FKegal88myrpa6m0jle9iv4p37t foreign key (function_id) references uc_functions (id)
    ) engine=InnoDB;

    create table if not exists uc_metastore (
        id binary(16) not null,
        primary key (id)
    ) engine=InnoDB;

    create table if not exists uc_model_versions (
        created_at datetime(6),
        updated_at datetime(6),
        version bigint,
        id binary(16) not null,
        registered_model_id binary(16),
        url varchar(4096),
        created_by varchar(255),
        owner varchar(255),
        run_id varchar(255),
        source varchar(255),
        status varchar(255),
        updated_by varchar(255),
        comment text,
        primary key (id),
        index idx_model_version (registered_model_id, version)
    ) engine=InnoDB;

    create table if not exists uc_properties (
        entity_id binary(16) not null,
        id binary(16) not null,
        entity_type varchar(255) not null,
        property_key varchar(255) not null,
        property_value mediumtext not null,
        primary key (id),
        constraint UKknh5y40jwdrv7qixutbl6c96r unique (entity_id, entity_type, property_key)
    ) engine=InnoDB;

    create table if not exists uc_registered_models (
        created_at datetime(6),
        max_version_number bigint,
        updated_at datetime(6),
        id binary(16) not null,
        schema_id binary(16),
        url varchar(4096),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        comment text,
        primary key (id),
        index uc_registered_models_name_idx (name)
    ) engine=InnoDB;

    create table if not exists uc_schemas (
        created_at datetime(6),
        updated_at datetime(6),
        catalog_id binary(16),
        id binary(16) not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        storage_root varchar(255),
        updated_by varchar(255),
        primary key (id)
    ) engine=InnoDB;

    -- Left out on purpose: idx_staging_location on uc_staging_tables (staging_location) exceeds
    -- MySQL's 3072-byte key limit (varchar(2048), utf8mb4), so Hibernate's create script failed on
    -- it and databases of the baseline release on MySQL never had it.
    create table if not exists uc_staging_tables (
        num_cleanup_retries smallint not null,
        purge_state smallint not null,
        stage_committed bit not null,
        accessed_at datetime(6) not null,
        created_at datetime(6) not null,
        last_cleanup_at datetime(6),
        stage_committed_at datetime(6),
        id binary(16) not null,
        schema_id binary(16),
        staging_location varchar(2048) not null,
        created_by varchar(255),
        name varchar(255) not null,
        primary key (id)
    ) engine=InnoDB;

    create table if not exists uc_storage_cleanup_tasks (
        failure_count integer not null,
        deleted_at datetime(6) not null,
        lease_expires_at datetime(6),
        lease_token binary(16),
        resource_id binary(16) not null,
        last_error varchar(2048),
        storage_location varchar(4096) not null,
        name varchar(255) not null,
        resource_type enum ('MODEL_VERSION','REGISTERED_MODEL','STAGING_TABLE','TABLE','VOLUME') not null,
        primary key (resource_id),
        index uc_storage_cleanup_tasks_deleted_idx (deleted_at),
        index uc_storage_cleanup_tasks_lease_idx (lease_expires_at)
    ) engine=InnoDB;

    create table if not exists uc_users (
        created_at datetime(6),
        updated_at datetime(6),
        id binary(16) not null,
        email varchar(255),
        external_id varchar(255),
        name varchar(255) not null,
        picture_url varchar(255),
        state varchar(255),
        primary key (id),
        constraint uc_users_external_id_key unique (external_id)
    ) engine=InnoDB;

    create table if not exists uc_volumes (
        created_at datetime(6),
        updated_at datetime(6),
        id binary(16) not null,
        schema_id binary(16),
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        updated_by varchar(255),
        volume_type varchar(255),
        primary key (id)
    ) engine=InnoDB;
