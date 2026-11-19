-- The S3 endpoint of an external location on S3-compatible storage; null for other storage.
ALTER TABLE uc_external_locations ADD COLUMN endpoint varchar(512);
