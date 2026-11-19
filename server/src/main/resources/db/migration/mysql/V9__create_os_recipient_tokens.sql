    -- os_recipient_tokens references os_recipients.

    create table if not exists os_recipient_tokens (
        activated bit not null,
        created_at datetime(6) not null,
        expires_at datetime(6) not null,
        superseded_at datetime(6),
        updated_at datetime(6) not null,
        activation_code varchar(36),
        id binary(16) not null,
        recipient_id binary(16) not null,
        token_hash varchar(64),
        primary key (id),
        constraint uk_recipient_tokens_activation_code unique (activation_code),
        constraint uk_recipient_tokens_token_hash unique (token_hash),
        constraint fk_recipient_tokens_recipient foreign key (recipient_id) references os_recipients (id)
    ) engine=InnoDB;
