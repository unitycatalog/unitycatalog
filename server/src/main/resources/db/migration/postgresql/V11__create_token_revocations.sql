CREATE TABLE uc_token_revocations (expires_at timestamp(6), jti uuid not null, primary key (jti));
