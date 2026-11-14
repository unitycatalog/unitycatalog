create table if not exists uc_token_revocations (
    expires_at timestamp(6),
    jti uuid not null,
    primary key (jti)
);

create index if not exists idx_token_revocations_expires_at
   on uc_token_revocations (expires_at);
