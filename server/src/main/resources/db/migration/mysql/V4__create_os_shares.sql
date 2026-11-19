    -- OpenSharing's tables, created on UC's database: see OpenSharing's V1 migration. One
    -- statement per step, the tables in foreign-key order.

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
