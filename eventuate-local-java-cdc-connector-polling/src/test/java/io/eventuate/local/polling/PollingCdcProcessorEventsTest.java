package io.eventuate.local.polling;

import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingCdcProcessorEventsTest extends CdcProcessorEventsTest {

}
