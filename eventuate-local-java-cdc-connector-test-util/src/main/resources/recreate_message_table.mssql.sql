DROP TABLE eventuate.message;

CREATE TABLE eventuate.message (
  id VARCHAR(767) PRIMARY KEY,
  headers NVARCHAR(MAX) NOT NULL,
  published SMALLINT DEFAULT 0,
  message_partition SMALLINT,
  creation_time BIGINT,
  destination NVARCHAR(MAX) NOT NULL,
  payload NVARCHAR(MAX) NOT NULL
);