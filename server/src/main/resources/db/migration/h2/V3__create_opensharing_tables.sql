    -- OpenSharing's tables, created on UC's database: see OpenSharing's V1 migration.

    create table if not exists os_shares (
        created_at timestamp(6) with time zone not null,
        updated_at timestamp(6) with time zone not null,
        id uuid not null,
        comment varchar(65536),
        display_name varchar(255),
        name varchar(255) not null,
        owner_id varchar(255) not null,
        primary key (id),
        constraint uk_shares_name unique (name)
    );

    create table if not exists os_share_properties (
        share_id uuid not null,
        property_value varchar(1000),
        property_key varchar(255) not null,
        constraint uk_share_properties primary key (share_id, property_key),
        constraint fk_share_properties_share foreign key (share_id) references os_shares (id)
    );

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

    create table if not exists os_recipients (
        created_at timestamp(6) with time zone not null,
        updated_at timestamp(6) with time zone not null,
        authentication_type varchar(255) not null,
        id uuid not null,
        comment varchar(65536),
        name varchar(255) not null,
        owner_id varchar(255) not null,
        primary key (id),
        constraint uk_recipients_name unique (name)
    );

    create table if not exists os_share_permissions (
        created_at timestamp(6) with time zone not null,
        updated_at timestamp(6) with time zone not null,
        privilege varchar(32) not null,
        id uuid not null,
        recipient_id uuid not null,
        share_id uuid not null,
        primary key (id),
        constraint uk_share_permission unique (share_id, recipient_id, privilege),
        constraint fk_share_permissions_recipient foreign key (recipient_id) references os_recipients (id),
        constraint fk_share_permissions_share foreign key (share_id) references os_shares (id)
    );

    create table if not exists os_recipient_tokens (
        activated boolean not null,
        created_at timestamp(6) with time zone not null,
        expires_at timestamp(6) with time zone not null,
        superseded_at timestamp(6) with time zone,
        updated_at timestamp(6) with time zone not null,
        activation_code varchar(36),
        id uuid not null,
        recipient_id uuid not null,
        token_hash varchar(64),
        primary key (id),
        constraint uk_recipient_tokens_activation_code unique (activation_code),
        constraint uk_recipient_tokens_token_hash unique (token_hash),
        constraint fk_recipient_tokens_recipient foreign key (recipient_id) references os_recipients (id)
    );

    -- The owner indexes go last: on MySQL a failed step cannot roll back, so a rerun after a
    -- repair has to work. The tables skip what is already there; MySQL has no "if not exists"
    -- for indexes, so the indexes only run once every table did.
    create index if not exists idx_shares_owner_id on os_shares (owner_id);
    create index if not exists idx_recipients_owner_id on os_recipients (owner_id);
