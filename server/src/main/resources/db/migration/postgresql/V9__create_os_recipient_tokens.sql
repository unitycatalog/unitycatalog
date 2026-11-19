    -- os_recipient_tokens references os_recipients.

    create table if not exists os_recipient_tokens (
        activated boolean not null,
        created_at timestamp(6) with time zone not null,
        expires_at timestamp(6) with time zone not null,
        superseded_at timestamp(6) with time zone,
        updated_at timestamp(6) with time zone not null,
        activation_code varchar(36),
        id uuid not null,
        recipient_id uuid not null,
        token_hash varchar(64),
        primary key (id),
        constraint uk_recipient_tokens_activation_code unique (activation_code),
        constraint uk_recipient_tokens_token_hash unique (token_hash),
        constraint fk_recipient_tokens_recipient foreign key (recipient_id) references os_recipients (id)
    );
