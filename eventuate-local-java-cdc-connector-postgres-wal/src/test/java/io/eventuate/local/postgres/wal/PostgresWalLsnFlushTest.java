package io.eventuate.local.postgres.wal;

import io.eventuate.common.jdbc.EventuateSchema;
import io.eventuate.util.test.async.Eventually;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnectionImpl;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.ReplicationStreamBuilder;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.*;

public class PostgresWalLsnFlushTest {

    @Test
    public void whenLsnOverDefaultLimitThenFlushLsn() throws SQLException {
        PGReplicationConnectionImpl replicationConnection = Mockito.mock(PGReplicationConnectionImpl.class);
        PGReplicationStream replicationStream = mockReplicationStream(replicationConnection);
        Connection connection = mockConnection(replicationConnection);
        Mockito.when(replicationStream.readPending()).thenReturn(null);

        LogSequenceNumber lsn1 = LogSequenceNumber.valueOf("C3/8C9C12B0");
        LogSequenceNumber lsn2 = LogSequenceNumber.valueOf("C2/E1649A00");

        Mockito.when(replicationStream.getLastReceiveLSN()).thenReturn(lsn1);
        Mockito.when(replicationStream.getLastFlushedLSN()).thenReturn(lsn2);

        PostgresConnectionFactory connectionFactory = Mockito.mock(PostgresConnectionFactory.class);
        Mockito.when(connectionFactory.create(any(), any())).thenReturn(connection);
        PostgresWalClient client = createPostgresWalClient(connectionFactory);

        runInSeparateThread(client::start);
        Eventually.eventually(() -> Mockito.verify(replicationStream, Mockito.times(1)).setFlushedLSN(lsn1));
        client.stop();
    }

    @Test
    public void whenLsnUnderDefaultLimitThenDoNotFlushLsn() throws SQLException {
        PGReplicationConnectionImpl replicationConnection = Mockito.mock(PGReplicationConnectionImpl.class);
        PGReplicationStream replicationStream = mockReplicationStream(replicationConnection);
        Connection connection = mockConnection(replicationConnection);
        Mockito.when(replicationStream.readPending()).thenReturn(null);

        LogSequenceNumber lsn1 = LogSequenceNumber.valueOf("C2/E1649A00");
        LogSequenceNumber lsn2 = LogSequenceNumber.valueOf("C2/8C9C12B0");

        Mockito.when(replicationStream.getLastReceiveLSN()).thenReturn(lsn1);
        Mockito.when(replicationStream.getLastFlushedLSN()).thenReturn(lsn2);

        PostgresConnectionFactory connectionFactory = Mockito.mock(PostgresConnectionFactory.class);
        Mockito.when(connectionFactory.create(any(), any())).thenReturn(connection);
        PostgresWalClient client = createPostgresWalClient(connectionFactory);

        runInSeparateThread(client::start);
        Eventually.eventually(() -> Mockito.verify(replicationStream, Mockito.times(0)).setFlushedLSN(any()));
        client.stop();
    }

    private void runInSeparateThread(Runnable callback) {
        new Thread(callback).start();
    }

    private Connection mockConnection(PGReplicationConnectionImpl replicationConnection) throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        PgConnection pgConnection = Mockito.mock(PgConnection.class);
        Mockito.when(pgConnection.getReplicationAPI()).thenReturn(replicationConnection);
        Mockito.when(connection.unwrap(any())).thenReturn(pgConnection);
        return connection;
    }

    private PGReplicationStream mockReplicationStream(PGReplicationConnectionImpl replicationConnection) throws SQLException {
        ReplicationStreamBuilder replicationStreamBuilder = Mockito.mock(ReplicationStreamBuilder.class);
        ChainedLogicalStreamBuilder chainedLogicalStreamBuilder = Mockito.spy(ChainedLogicalStreamBuilder.class);
        Mockito.when(chainedLogicalStreamBuilder.withSlotName(any())).thenReturn(chainedLogicalStreamBuilder);
        Mockito.when(chainedLogicalStreamBuilder.withSlotOption(any(), anyBoolean())).thenReturn(chainedLogicalStreamBuilder);
        Mockito.when(chainedLogicalStreamBuilder.withStatusInterval(anyInt(), any())).thenReturn(chainedLogicalStreamBuilder);
        Mockito.when(replicationConnection.replicationStream()).thenReturn(replicationStreamBuilder);
        Mockito.when(replicationStreamBuilder.logical()).thenReturn(chainedLogicalStreamBuilder);
        PGReplicationStream replicationStream = Mockito.mock(PGReplicationStream.class);
        Mockito.when(chainedLogicalStreamBuilder.start()).thenReturn(replicationStream);
        return replicationStream;
    }

    private PostgresWalClient createPostgresWalClient(PostgresConnectionFactory connectionFactory) {
        return new PostgresWalClient(
                new LoggingMeterRegistry(),
                "jdbc:postgresql://localhost:5432/eventuate",
                "postgres",
                "postgres",
                1000,
                1000,
                1000,
                1000,
                "test_slot",
                Mockito.mock(DataSource.class),
                "test_reader",
                1000,
                1000,
                1000,
                "test_additional_slot",
                1000,
                new EventuateSchema(EventuateSchema.DEFAULT_SCHEMA),
                1L,
                1000,
                connectionFactory
        );
    }
}
