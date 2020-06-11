USE eventuate;
GO

ALTER TABLE eventuate.message
    ADD CONSTRAINT [headers should be formatted as JSON] CHECK (ISJSON(headers)=1);
GO
