CREATE TABLE eventuate.new_message (
  id VARCHAR(1000),
  xid bigserial PRIMARY KEY,
  destination TEXT NOT NULL,
  headers TEXT NOT NULL,
  payload TEXT NOT NULL,
  published SMALLINT DEFAULT 0,
  creation_time BIGINT
);

INSERT INTO eventuate.new_message (id, destination, headers, payload, published, creation_time) SELECT '', destination, headers, payload, published, creation_time FROM eventuate.message;

DROP TABLE eventuate.message;

ALTER TABLE eventuate.new_message RENAME TO message;



