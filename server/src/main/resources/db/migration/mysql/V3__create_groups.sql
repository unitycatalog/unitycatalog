create table uc_groups (
    created_at datetime(6),
    updated_at datetime(6),
    id binary(16) not null,
    display_name varchar(255) not null,
    external_id varchar(255),
    primary key (id),
    constraint uc_groups_display_name_key unique (display_name)
) engine=InnoDB;

create table uc_group_members (
    group_id binary(16) not null,
    user_id binary(16) not null,
    primary key (group_id, user_id),
    index uc_group_members_user_idx (user_id)
) engine=InnoDB;
