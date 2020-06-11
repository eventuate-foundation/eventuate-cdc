ALTER TABLE eventuate.message DROP COLUMN payload;
ALTER TABLE eventuate.message ADD COLUMN payload JSON;

ALTER TABLE eventuate.message DROP COLUMN headers;
ALTER TABLE eventuate.message ADD COLUMN headers JSON;
