package io.eventuate.local.polling;

import io.eventuate.local.test.util.CdcProcessorEventsTest;
import io.eventuate.local.testutil.DefaultAndPollingProfilesResolver;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@ActiveProfiles(resolver = DefaultAndPollingProfilesResolver.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PollingIntegrationTestConfiguration.class)
public class PollingCdcProcessorEventsTest extends CdcProcessorEventsTest {

}
