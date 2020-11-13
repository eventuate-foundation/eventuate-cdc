ALTER TABLE eventuate.message DROP destination;
ALTER TABLE eventuate.message DROP payload;
ALTER TABLE eventuate.message ADD destination LONGTEXT;
ALTER TABLE eventuate.message ADD payload LONGTEXT;