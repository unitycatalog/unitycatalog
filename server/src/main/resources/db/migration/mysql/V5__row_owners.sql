-- Rows that exist before this step were written by the admin.
alter table uc_users add column owner varchar(255) not null default 'admin';
alter table uc_users alter column owner drop default;
alter table uc_groups add column owner varchar(255) not null default 'admin';
alter table uc_groups alter column owner drop default;
alter table uc_group_members add column admin_added bit not null default 1;
alter table uc_group_members alter column admin_added drop default;
-- Hibernate orders the key columns by name, so the group's lookups get their own index.
alter table uc_group_members drop primary key, add primary key (admin_added, group_id, user_id),
    add index uc_group_members_group_idx (group_id);
