DROP TABLE eventuate.message;

CREATE TABLE message (
  id VARCHAR(767) PRIMARY KEY,
  destination LONGTEXT NOT NULL,
  headers LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  published SMALLINT DEFAULT 0,
  creation_time BIGINT
);