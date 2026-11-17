ALTER TABLE uc_credentials MODIFY COLUMN credential_type enum ('AWS_IAM_ROLE', 'GCP_SERVICE_ACCOUNT_KEY', 'S3_COMPATIBLE') not null;
