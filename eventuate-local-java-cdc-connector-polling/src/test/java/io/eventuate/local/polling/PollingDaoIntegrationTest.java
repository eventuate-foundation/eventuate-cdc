package io.eventuate.local.polling;

import io.eventuate.common.jdbc.OutboxTableSuffix;
import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.polling.spec.PollingSpec;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = AbstractPollingDaoIntegrationTest.Config.class)
@EnableAutoConfiguration
public class PollingDaoIntegrationTest extends AbstractPollingDaoIntegrationTest {

    private final OutboxTableSuffix messageTableSuffix = new OutboxTableSuffix(null); // TODO

    @Test
    public void testThatPollingEventCountAreLimited() {
        BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(CompletableFuture.completedFuture(null));

        List<String> eventIds = saveEvents();

        pollingDao.processEvents(binlogEntryHandler, PollingSpec.ALL, messageTableSuffix);

        assertEquals(EVENTS_PER_POLLING_ITERATION, processedEvents.get());

        assertEventsArePublished(eventIds.subList(0, EVENTS_PER_POLLING_ITERATION));
    }

    @Test
    public void testMessagesAreNotProcessedTwice() throws InterruptedException {
        CompletableFuture<?> completableFuture = new CompletableFuture();

        BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(completableFuture);

        saveEvents();

        CountDownLatch allIterationsComplete = new CountDownLatch(1);

        CompletableFuture.supplyAsync(() -> {
            try {
                for (int i = 0; i < (NUMBER_OF_EVENTS_TO_PUBLISH / EVENTS_PER_POLLING_ITERATION) * 2; i++) {
                    pollingDao.processEvents(binlogEntryHandler, PollingSpec.ALL, messageTableSuffix);
                }
                allIterationsComplete.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        Thread.sleep(3000);
        completableFuture.complete(null);
        allIterationsComplete.await(30, TimeUnit.SECONDS);

        assertEquals(NUMBER_OF_EVENTS_TO_PUBLISH, processedEvents.get());
    }

    @Test
    public void shouldHaveEmptyListOfParallelChannels() {
        assertEquals(0, eventuateConfigurationProperties.getPollingParallelChannels().length);
    }

}
