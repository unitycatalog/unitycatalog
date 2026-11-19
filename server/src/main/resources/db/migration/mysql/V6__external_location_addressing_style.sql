-- How requests to the endpoint of an external location name the bucket (S3AddressingStyle); null
-- without an endpoint.
ALTER TABLE uc_external_locations ADD COLUMN addressing_style varchar(32);
