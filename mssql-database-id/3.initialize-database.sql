USE eventuate;
GO

CREATE TABLE eventuate.new_message (
  id VARCHAR(767),
  xid BIGINT IDENTITY(1,1) PRIMARY KEY,
  destination NVARCHAR(MAX) NOT NULL,
  headers NVARCHAR(MAX) NOT NULL,
  payload NVARCHAR(MAX) NOT NULL,
  published SMALLINT DEFAULT 0,
  creation_time BIGINT
);
GO

INSERT INTO eventuate.new_message (id, destination, headers, payload, published, creation_time) SELECT '', destination, headers, payload, published, creation_time FROM eventuate.message;
GO

DROP TABLE eventuate.message;
GO

EXEC sp_rename 'eventuate.new_message', 'message';
GO

