ALTER TABLE uc_credentials DROP CONSTRAINT uc_credentials_credential_type_check;
ALTER TABLE uc_credentials ADD CONSTRAINT uc_credentials_credential_type_check CHECK (credential_type IN ('AWS_IAM_ROLE', 'AZURE_SERVICE_PRINCIPAL', 'GCP_SERVICE_ACCOUNT_KEY'));
