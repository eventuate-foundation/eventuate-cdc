package io.eventuate.local.polling;

import io.eventuate.common.jdbc.OutboxPartitioningSpec;
import io.eventuate.common.testcontainers.EventuateMySqlContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.tram.cdc.connector.BinlogEntryToMessageConverter;
import io.eventuate.util.test.async.Eventually;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MultipleOutboxPollingDaoIntegrationTest.Config.class, properties = {"eventuate.cdc.outbox.partitioning.outbox.tables=8", "eventuate.cdc.outbox.partitioning.message.partitions=4"})
@EnableAutoConfiguration
public class MultipleOutboxPollingDaoIntegrationTest extends AbstractPollingDaoIntegrationTest {
    public static final int OUTBOX_TABLES = 8;

    @Autowired
    private OutboxPartitioningSpec outboxPartitioningSpec;

    @Configuration
    @Import(AbstractPollingDaoIntegrationTest.Config.class)
    public static class Config {

        @Bean
        public OutboxPartitioningSpec outboxPartitioningSpec() {
            return new OutboxPartitioningSpec(OUTBOX_TABLES, 4);
        }

    }

    private ExecutorService executor = Executors.newCachedThreadPool();

    public static EventuateMySqlContainer mysql =
            new EventuateMySqlContainer()
                    .withEnv("EVENTUATE_OUTBOX_TABLES", Integer.toString(OUTBOX_TABLES))
                    .withReuse(true);

    @DynamicPropertySource
    static void registerMySqlProperties(DynamicPropertyRegistry registry) {
        PropertyProvidingContainer.startAndProvideProperties(registry, mysql);
    }

    private String sendMessage() {
        String rawPayload = "\"" + "payload-" + testHelper.generateId() + "\"";
        return testHelper.saveMessage(idGenerator, rawPayload, testHelper.generateId(), Collections.singletonMap("PARTITION_ID", UUID.randomUUID().toString()), eventuateSchema);
    }

    @Test
    public void testPolling() throws Exception {
        BinlogEntryHandler binlogEntryHandler = pollingDao.addBinlogEntryHandler(eventuateSchema,
                "message",
                new BinlogEntryToMessageConverter(idGenerator),
                event -> {
                    processedEvents.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });

        List<String> messageIds = IntStream.range(0, NUMBER_OF_EVENTS_TO_PUBLISH).mapToObj(i -> sendMessage()).collect(Collectors.toList());

        Future<?> f = executor.submit(() -> pollingDao.start());

        Eventually.eventually(() -> {
            assertThat(processedEvents.get()).isGreaterThanOrEqualTo(NUMBER_OF_EVENTS_TO_PUBLISH);
            assertMessagesPublished(messageIds);
        });

        pollingDao.stop();
        f.get(1, TimeUnit.SECONDS);

    }

    private void assertMessagesPublished(List<String> messageIds) {
        messageIds.forEach(this::assertMessagePublished);
    }

    private void assertMessagePublished(String id) {
        assertEquals(1, outboxPartitioningSpec.outboxTableSuffixes().stream().map(suffix -> {
                    Map<String, Object> count = jdbcTemplate.queryForMap(String.format("select count(*) as c from %s%s where id = ? and published = 1",
                            eventuateSchema.qualifyTable("message"), suffix), id);
                    return ((Number) count.get("c")).intValue();
                }).reduce(0, Integer::sum).intValue());
    }


}
