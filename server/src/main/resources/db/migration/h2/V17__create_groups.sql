create table uc_groups (
    created_at timestamp(6),
    updated_at timestamp(6),
    id uuid not null,
    display_name varchar(255) not null,
    external_id varchar(255),
    primary key (id),
    constraint uc_groups_display_name_key unique (display_name)
);

create table uc_group_members (
    group_id uuid not null,
    user_id uuid not null,
    primary key (group_id, user_id)
);

create index uc_group_members_user_idx
    on uc_group_members (user_id);
