package io.eventuate.local.mysql.binlog;

import io.eventuate.local.test.util.TestHelper;
import io.eventuate.util.test.async.Eventually;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/*
* Test checks that mysql reader is restarted in case if reading or writing binlog offset completed exceptionally.
* i.e. if OffsetStore.save or OffsetStore.getLastBinlogFileOffset throws exception.
*/

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MySqlBinlogCdcIntegrationTestConfiguration.class,
        OffsetStoreMockConfiguration.class})
public class MySqlBinaryLogClientRestartOnOffsetHandlingExceptionTest {

  @Autowired
  protected MySqlBinaryLogClient mySqlBinaryLogClient;

  @Autowired
  protected TestHelper testHelper;

  @Autowired
  protected OffsetStoreMock offsetStoreMock;

  @Before
  public void init() {
    offsetStoreMock.throwExceptionOnSave = false;
    offsetStoreMock.throwExceptionOnLoad = false;
  }

  @Test
  public void testRestartFailedOffsetLoad() {
    offsetStoreMock.throwExceptionOnLoad = true;

    testRestart();
  }

  @Test
  public void testRestartFailedOffsetSave() {
    offsetStoreMock.throwExceptionOnSave = true;

    testRestart();
  }

  private void testRestart() {

    Runnable restartCallback = Mockito.mock(Runnable.class);

    mySqlBinaryLogClient.setRestartCallback(restartCallback);

    testHelper.runInSeparateThread(mySqlBinaryLogClient::start);

    Eventually.eventually(() -> {
      Mockito.verify(restartCallback).run();
    });

    mySqlBinaryLogClient.stop();
  }
}
