-- Credentials are stored as text instead of large objects. The conversion reads each large object; the
-- large objects themselves stay until vacuumlo removes them (see docs/server/upgrades.md).
ALTER TABLE uc_credentials ALTER COLUMN credential TYPE text USING convert_from(lo_get(credential), 'UTF8');
