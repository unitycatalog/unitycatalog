    -- OpenSharing's tables, created on UC's database: see OpenSharing's V1 migration.

    create table if not exists os_shares (
        created_at datetime(6) not null,
        updated_at datetime(6) not null,
        id binary(16) not null,
        comment mediumtext,
        display_name varchar(255),
        name varchar(255) not null,
        owner_id varchar(255) not null,
        primary key (id),
        constraint uk_shares_name unique (name)
    ) engine=InnoDB;

    create table if not exists os_share_properties (
        share_id binary(16) not null,
        property_value varchar(1000),
        property_key varchar(255) not null,
        constraint uk_share_properties primary key (share_id, property_key),
        constraint fk_share_properties_share foreign key (share_id) references os_shares (id)
    ) engine=InnoDB;

    create table if not exists os_shared_data_objects (
        created_at datetime(6) not null,
        updated_at datetime(6) not null,
        source_format varchar(255),
        type varchar(255) not null,
        id binary(16) not null,
        share_id binary(16) not null,
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
    ) engine=InnoDB;

    create table if not exists os_recipients (
        created_at datetime(6) not null,
        updated_at datetime(6) not null,
        authentication_type varchar(255) not null,
        id binary(16) not null,
        comment mediumtext,
        name varchar(255) not null,
        owner_id varchar(255) not null,
        primary key (id),
        constraint uk_recipients_name unique (name)
    ) engine=InnoDB;

    create table if not exists os_share_permissions (
        created_at datetime(6) not null,
        updated_at datetime(6) not null,
        privilege varchar(32) not null,
        id binary(16) not null,
        recipient_id binary(16) not null,
        share_id binary(16) not null,
        primary key (id),
        constraint uk_share_permission unique (share_id, recipient_id, privilege),
        constraint fk_share_permissions_recipient foreign key (recipient_id) references os_recipients (id),
        constraint fk_share_permissions_share foreign key (share_id) references os_shares (id)
    ) engine=InnoDB;

    create table if not exists os_recipient_tokens (
        activated bit not null,
        created_at datetime(6) not null,
        expires_at datetime(6) not null,
        superseded_at datetime(6),
        updated_at datetime(6) not null,
        activation_code varchar(36),
        id binary(16) not null,
        recipient_id binary(16) not null,
        token_hash varchar(64),
        primary key (id),
        constraint uk_recipient_tokens_activation_code unique (activation_code),
        constraint uk_recipient_tokens_token_hash unique (token_hash),
        constraint fk_recipient_tokens_recipient foreign key (recipient_id) references os_recipients (id)
    ) engine=InnoDB;

    -- The owner indexes go last: on MySQL a failed step cannot roll back, so a rerun after a
    -- repair has to work. The tables skip what is already there; MySQL has no "if not exists"
    -- for indexes, so the indexes only run once every table did.
    create index idx_shares_owner_id on os_shares (owner_id);
    create index idx_recipients_owner_id on os_recipients (owner_id);
