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
