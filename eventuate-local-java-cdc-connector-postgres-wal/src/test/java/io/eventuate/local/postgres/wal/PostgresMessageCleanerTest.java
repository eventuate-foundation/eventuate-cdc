package io.eventuate.local.postgres.wal;

import io.eventuate.local.test.util.AbstractMessageCleanerTest;
import io.eventuate.local.testutil.DefaultAndPostgresProfilesResolver;
import io.eventuate.local.testutil.DefaultAndPostgresWalProfilesResolver;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AbstractMessageCleanerTest.Config.class)
@ActiveProfiles(resolver = DefaultAndPostgresProfilesResolver.class)
public class PostgresMessageCleanerTest extends AbstractMessageCleanerTest {

}
