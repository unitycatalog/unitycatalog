-- Rows that exist before this step were written by the admin.
alter table uc_users add column owner varchar(255) default 'admin' not null;
alter table uc_users alter column owner drop default;
alter table uc_groups add column owner varchar(255) default 'admin' not null;
alter table uc_groups alter column owner drop default;
alter table uc_group_members add column admin_added boolean default true not null;
alter table uc_group_members alter column admin_added drop default;
alter table uc_group_members drop primary key;
-- Hibernate orders the key columns by name, so the group's lookups get their own index.
alter table uc_group_members add primary key (admin_added, group_id, user_id);
create index uc_group_members_group_idx on uc_group_members (group_id);
