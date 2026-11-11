ALTER TABLE uc_users ADD COLUMN link_issuer varchar(255);
ALTER TABLE uc_users ADD COLUMN link_claim varchar(16);
ALTER TABLE uc_users ADD COLUMN link_value varchar(255);
ALTER TABLE uc_users ADD CONSTRAINT uc_users_link_key UNIQUE (link_issuer, link_claim, link_value);
ALTER TABLE uc_users ADD COLUMN reactivated_at timestamp(6);
