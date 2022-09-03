DROP TABLE eventuate.message;

CREATE TABLE eventuate.message (
  id VARCHAR(1000) PRIMARY KEY,
  headers TEXT NOT NULL,
  published SMALLINT DEFAULT 0,
  message_partition SMALLINT,
  creation_time BIGINT,
  destination TEXT NOT NULL,
  payload TEXT NOT NULL
);