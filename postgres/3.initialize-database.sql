SELECT * FROM pg_create_logical_replication_slot('eventuate_slot', 'wal2json');
SELECT * FROM pg_create_logical_replication_slot('eventuate_slot2', 'wal2json');