ALTER TABLE uc_credentials MODIFY COLUMN credential_type enum ('AWS_IAM_ROLE', 'S3_COMPATIBLE') not null;
