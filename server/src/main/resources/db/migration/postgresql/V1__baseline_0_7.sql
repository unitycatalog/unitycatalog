
    create table uc_catalogs (
        created_at timestamp(6),
        updated_at timestamp(6),
        id uuid not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        storage_root varchar(255),
        updated_by varchar(255),
        primary key (id)
    );

    create table uc_columns (
        nullable boolean not null,
        ordinal_position smallint not null,
        partition_index smallint,
        type_precision integer,
        type_scale integer,
        id uuid not null,
        table_id uuid not null,
        type_name varchar(32) not null,
        comment varchar(65535),
        name varchar(255) not null,
        type_interval_type varchar(255),
        type_json text not null,
        type_text oid not null,
        primary key (id),
        unique (table_id, ordinal_position, name)
    );

    create table uc_credentials (
        purpose smallint not null check (purpose between 0 and 0),
        created_at timestamp(6) not null,
        updated_at timestamp(6),
        id uuid not null,
        comment varchar(255),
        created_by varchar(255),
        credential_type varchar(255) not null check (credential_type in ('AWS_IAM_ROLE')),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        credential oid not null,
        primary key (id)
    );

    create table uc_delta_commits (
        is_backfilled_latest_commit boolean not null,
        commit_file_modification_timestamp timestamp(6) not null,
        commit_filesize bigint not null,
        commit_timestamp timestamp(6) not null,
        commit_version bigint not null,
        id uuid not null,
        table_id uuid not null,
        commit_filename varchar(255) not null,
        primary key (id),
        unique (table_id, commit_version)
    );

    create table uc_dependencies (
        dependent_id uuid not null,
        id uuid not null,
        dependency_catalog varchar(255),
        dependency_name varchar(255),
        dependency_schema varchar(255),
        dependency_type varchar(255) not null check (dependency_type in ('TABLE','FUNCTION')),
        dependent_type varchar(255) not null check (dependent_type in ('TABLE')),
        primary key (id)
    );

    create table uc_external_locations (
        created_at timestamp(6),
        updated_at timestamp(6),
        credential_id uuid not null,
        id uuid not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        url varchar(255) not null,
        primary key (id)
    );

    create table uc_function_params (
        input_or_return smallint not null check (input_or_return between 0 and 1),
        parameter_mode smallint check (parameter_mode between 0 and 0),
        parameter_type smallint check (parameter_type between 0 and 1),
        position integer,
        type_name smallint not null check (type_name between 0 and 21),
        type_precision integer,
        type_scale integer,
        function_id uuid,
        id uuid not null,
        comment varchar(255),
        name varchar(255) not null,
        parameter_default varchar(255),
        type_interval_type varchar(255),
        type_json varchar(255),
        type_text varchar(255) not null,
        primary key (id)
    );

    create table uc_functions (
        data_type smallint check (data_type between 0 and 21),
        is_deterministic boolean,
        is_null_call boolean,
        parameter_style smallint check (parameter_style between 0 and 0),
        routine_body smallint check (routine_body between 0 and 1),
        security_type smallint check (security_type between 0 and 0),
        sql_data_access smallint check (sql_data_access between 0 and 2),
        created_at bigint,
        updated_at bigint,
        id uuid not null,
        schema_id uuid,
        comment varchar(65535),
        created_by varchar(255),
        external_language varchar(255),
        full_data_type varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        specific_name varchar(255),
        updated_by varchar(255),
        routine_definition oid,
        primary key (id)
    );

    create table uc_metastore (
        id uuid not null,
        primary key (id)
    );

    create table uc_model_versions (
        created_at timestamp(6),
        updated_at timestamp(6),
        version bigint,
        id uuid not null,
        registered_model_id uuid,
        url varchar(4096),
        comment varchar(65535),
        created_by varchar(255),
        owner varchar(255),
        run_id varchar(255),
        source varchar(255),
        status varchar(255),
        updated_by varchar(255),
        primary key (id)
    );

    create table uc_properties (
        entity_id uuid not null,
        id uuid not null,
        entity_type varchar(255) not null,
        property_key varchar(255) not null,
        property_value text not null,
        primary key (id),
        unique (entity_id, entity_type, property_key)
    );

    create table uc_registered_models (
        created_at timestamp(6),
        max_version_number bigint,
        updated_at timestamp(6),
        id uuid not null,
        schema_id uuid,
        url varchar(4096),
        comment varchar(65535),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        updated_by varchar(255),
        primary key (id)
    );

    create table uc_schemas (
        created_at timestamp(6),
        updated_at timestamp(6),
        catalog_id uuid,
        id uuid not null,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        storage_root varchar(255),
        updated_by varchar(255),
        primary key (id)
    );

    create table uc_staging_tables (
        num_cleanup_retries smallint not null,
        purge_state smallint not null,
        stage_committed boolean not null,
        accessed_at timestamp(6) not null,
        created_at timestamp(6) not null,
        last_cleanup_at timestamp(6),
        stage_committed_at timestamp(6),
        id uuid not null,
        schema_id uuid,
        staging_location varchar(2048) not null,
        created_by varchar(255),
        name varchar(255) not null,
        primary key (id)
    );

    create table uc_storage_cleanup_tasks (
        failure_count integer not null,
        deleted_at timestamp(6) not null,
        lease_expires_at timestamp(6),
        lease_token uuid,
        resource_id uuid not null,
        last_error varchar(2048),
        storage_location varchar(4096) not null,
        name varchar(255) not null,
        resource_type varchar(255) not null check (resource_type in ('TABLE','VOLUME','REGISTERED_MODEL','MODEL_VERSION','STAGING_TABLE')),
        primary key (resource_id)
    );

    create table uc_tables (
        column_count integer,
        created_at timestamp(6),
        uniform_iceberg_converted_delta_timestamp timestamp(6),
        uniform_iceberg_converted_delta_version bigint,
        updated_at timestamp(6),
        id uuid not null,
        schema_id uuid,
        url varchar(2048),
        comment varchar(65535),
        uniform_iceberg_metadata_location varchar(65535),
        created_by varchar(255),
        data_source_format varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        type varchar(255),
        updated_by varchar(255),
        view_definition oid,
        primary key (id)
    );

    create table uc_users (
        created_at timestamp(6),
        updated_at timestamp(6),
        id uuid not null,
        email varchar(255),
        external_id varchar(255),
        name varchar(255) not null,
        picture_url varchar(255),
        state varchar(255),
        primary key (id),
        constraint uc_users_external_id_key unique (external_id)
    );

    create table uc_volumes (
        created_at timestamp(6),
        updated_at timestamp(6),
        id uuid not null,
        schema_id uuid,
        comment varchar(255),
        created_by varchar(255),
        name varchar(255) not null,
        owner varchar(255),
        storage_location varchar(255),
        updated_by varchar(255),
        volume_type varchar(255),
        primary key (id)
    );

    create index idx_dependent 
       on uc_dependencies (dependent_type, dependent_id);

    create index idx_url 
       on uc_external_locations (url);

    create index idx_credential_id 
       on uc_external_locations (credential_id);

    create index idx_model_version 
       on uc_model_versions (registered_model_id, version);

    create index uc_registered_models_name_idx 
       on uc_registered_models (name);

    create index idx_staging_location 
       on uc_staging_tables (staging_location);

    create index uc_storage_cleanup_tasks_deleted_idx 
       on uc_storage_cleanup_tasks (deleted_at);

    create index uc_storage_cleanup_tasks_lease_idx 
       on uc_storage_cleanup_tasks (lease_expires_at);

    create index idx_name 
       on uc_tables (name);

    alter table if exists uc_columns 
       add constraint FK1xslfe25li3uopodup6mylm3l 
       foreign key (table_id) 
       references uc_tables;

    alter table if exists uc_function_params 
       add constraint FKegal88myrpa6m0jle9iv4p37t 
       foreign key (function_id) 
       references uc_functions;
