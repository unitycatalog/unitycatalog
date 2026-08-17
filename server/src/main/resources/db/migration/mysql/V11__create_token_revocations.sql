CREATE TABLE uc_token_revocations (expires_at datetime(6), jti binary(16) not null, primary key (jti)) engine=InnoDB;
