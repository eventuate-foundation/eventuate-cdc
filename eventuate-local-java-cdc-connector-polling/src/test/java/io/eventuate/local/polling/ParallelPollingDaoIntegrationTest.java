package io.eventuate.local.polling;

import io.eventuate.local.common.BinlogEntryHandler;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = AbstractPollingDaoIntegrationTest.Config.class, properties = "eventuatelocal.cdc.polling.parallel.channels=x,y")
@EnableAutoConfiguration
public class ParallelPollingDaoIntegrationTest extends AbstractPollingDaoIntegrationTest {

    private ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testParallelPolling() throws ExecutionException, InterruptedException, TimeoutException {
        BinlogEntryHandler binlogEntryHandler = prepareBinlogEntryHandler(CompletableFuture.completedFuture(null));

        List<String> eventIds = saveEvents();

        Future<?> f = executor.submit(() -> pollingDao.start());

        Eventually.eventually(() -> {
            assertEquals(NUMBER_OF_EVENTS_TO_PUBLISH, processedEvents.get());
            assertEventsArePublished(eventIds);
        });

        pollingDao.stop();
        f.get(1, TimeUnit.SECONDS);

    }

    @Test
    public void shouldHaveNonEmptyListOfParallelChannels() {
        String[] pollingParallelChannels = eventuateConfigurationProperties.getPollingParallelChannels();
        assertEquals(2, pollingParallelChannels.length);
        assertEquals("x", pollingParallelChannels[0]);
        assertEquals("y", pollingParallelChannels[1]);
    }

}
