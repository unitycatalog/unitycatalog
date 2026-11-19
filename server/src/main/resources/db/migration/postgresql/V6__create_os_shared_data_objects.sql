    -- os_shared_data_objects references os_shares.

    create table if not exists os_shared_data_objects (
        created_at timestamp(6) with time zone not null,
        updated_at timestamp(6) with time zone not null,
        source_format varchar(255),
        type varchar(255) not null,
        id uuid not null,
        share_id uuid not null,
        full_name varchar(512) not null,
        storage_location varchar(2048),
        auxiliary_locations varchar(8192),
        shared_as_schema varchar(255) not null,
        shared_as_table varchar(255) not null,
        source_asset_id varchar(255),
        primary key (id),
        constraint uk_shared_objects_source unique (share_id, full_name),
        constraint uk_shared_objects_alias unique (share_id, shared_as_schema, shared_as_table),
        constraint fk_shared_objects_share foreign key (share_id) references os_shares (id)
    );
