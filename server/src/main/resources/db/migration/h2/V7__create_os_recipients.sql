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
