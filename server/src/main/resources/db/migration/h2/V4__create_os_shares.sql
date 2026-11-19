    -- OpenSharing's tables, created on UC's database: see OpenSharing's V1 migration. One
    -- statement per step, the tables in foreign-key order.

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
