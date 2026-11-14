-- Credentials can exceed tinytext's 255 bytes, so use mediumtext, the size of the other large text columns.
ALTER TABLE uc_credentials MODIFY COLUMN credential mediumtext NOT NULL;
