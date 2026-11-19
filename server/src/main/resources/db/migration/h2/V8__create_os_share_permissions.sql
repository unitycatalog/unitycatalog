    -- os_share_permissions references os_shares and os_recipients.

    create table if not exists os_share_permissions (
        created_at timestamp(6) with time zone not null,
        updated_at timestamp(6) with time zone not null,
        privilege varchar(32) not null,
        id uuid not null,
        recipient_id uuid not null,
        share_id uuid not null,
        primary key (id),
        constraint uk_share_permission unique (share_id, recipient_id, privilege),
        constraint fk_share_permissions_recipient foreign key (recipient_id) references os_recipients (id),
        constraint fk_share_permissions_share foreign key (share_id) references os_shares (id)
    );
