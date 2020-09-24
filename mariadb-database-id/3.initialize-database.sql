USE eventuate;

CREATE TABLE new_message (
  id VARCHAR(767),
  xid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  destination LONGTEXT NOT NULL,
  headers LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  published SMALLINT DEFAULT 0,
  creation_time BIGINT
);

INSERT INTO new_message (id, destination, headers, payload, published, creation_time) SELECT '', destination, headers, payload, published, creation_time FROM message;

DROP TABLE message;

ALTER TABLE new_message RENAME TO message;



