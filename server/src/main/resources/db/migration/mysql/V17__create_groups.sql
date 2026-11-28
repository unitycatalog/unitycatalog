create table uc_groups (
    created_at datetime(6),
    updated_at datetime(6),
    id binary(16) not null,
    display_name varchar(255) not null,
    external_id varchar(255),
    primary key (id),
    constraint uc_groups_display_name_key unique (display_name)
) engine=InnoDB;
