create table uc_group_members (
    group_id uuid not null,
    user_id uuid not null,
    primary key (group_id, user_id)
);
