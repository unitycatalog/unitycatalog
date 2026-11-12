    -- os_share_permissions references os_shares and os_recipients.

    create table if not exists os_share_permissions (
        created_at datetime(6) not null,
        updated_at datetime(6) not null,
        privilege varchar(32) not null,
        id binary(16) not null,
        recipient_id binary(16) not null,
        share_id binary(16) not null,
        primary key (id),
        constraint uk_share_permission unique (share_id, recipient_id, privilege),
        constraint fk_share_permissions_recipient foreign key (recipient_id) references os_recipients (id),
        constraint fk_share_permissions_share foreign key (share_id) references os_shares (id)
    ) engine=InnoDB;
