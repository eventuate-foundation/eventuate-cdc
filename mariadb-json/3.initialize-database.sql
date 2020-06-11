USE eventuate;

ALTER TABLE eventuate.message MODIFY payload JSON;
ALTER TABLE eventuate.message MODIFY headers JSON;