create table uc_group_members (
    group_id binary(16) not null,
    user_id binary(16) not null,
    primary key (group_id, user_id)
) engine=InnoDB;
