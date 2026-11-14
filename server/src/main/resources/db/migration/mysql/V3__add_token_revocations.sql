create table if not exists uc_token_revocations (
    expires_at datetime(6),
    jti binary(16) not null,
    primary key (jti),
    index idx_token_revocations_expires_at (expires_at)
) engine=InnoDB;
