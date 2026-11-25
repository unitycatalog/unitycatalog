-- One statement, so that the table is never without the CHECK of the credential types.
ALTER TABLE uc_credentials
    DROP CONSTRAINT uc_credentials_credential_type_check,
    ADD CONSTRAINT uc_credentials_credential_type_check
        CHECK (credential_type IN ('AWS_IAM_ROLE', 'GCP_SERVICE_ACCOUNT_KEY', 'S3_COMPATIBLE'));
