ALTER TABLE eventuate.message DROP COLUMN destination;
ALTER TABLE eventuate.message DROP COLUMN payload;
ALTER TABLE eventuate.message ADD destination NVARCHAR(MAX);
ALTER TABLE eventuate.message ADD payload NVARCHAR(MAX);