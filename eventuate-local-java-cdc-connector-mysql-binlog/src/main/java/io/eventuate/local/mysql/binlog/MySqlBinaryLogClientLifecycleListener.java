package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlBinaryLogClientLifecycleListener implements BinaryLogClient.LifecycleListener {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private String readerName;

  public MySqlBinaryLogClientLifecycleListener(String readerName) {
    this.readerName = readerName;
  }

  @Override
  public void onConnect(BinaryLogClient client) {
    logger.info("Reader {} connected.", readerName);
  }

  @Override
  public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
    logger.error("Reader {} communication failed.", readerName);
    logger.error("Reader communication failed.", ex);
  }

  @Override
  public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
    logger.error("Reader {} deserialization failed.", readerName);
    logger.error("Reader deserialization failed.", ex);
  }

  @Override
  public void onDisconnect(BinaryLogClient client) {
    logger.info("Reader {} disconnected.", readerName);
  }
}
