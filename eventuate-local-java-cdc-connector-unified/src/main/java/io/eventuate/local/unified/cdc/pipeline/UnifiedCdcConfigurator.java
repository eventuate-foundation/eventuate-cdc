package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.coordination.leadership.LeaderSelectorFactory;
import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.common.BinlogEntryReaderLeadership;
import io.eventuate.local.mysql.binlog.MySqlBinaryLogClient;
import io.eventuate.local.unified.cdc.pipeline.common.BinlogEntryReaderProvider;
import io.eventuate.local.unified.cdc.pipeline.common.CdcPipeline;
import io.eventuate.local.unified.cdc.pipeline.common.DefaultSourceTableNameResolver;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineFactory;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.stream.Collectors;

public class UnifiedCdcConfigurator {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private List<CdcPipeline> cdcPipelines = new ArrayList<>();

  @Value("${eventuate.cdc.service.dry.run:#{false}}")
  private boolean dryRunOption;

  @Autowired
  private Collection<CdcPipelineFactory> cdcPipelineFactories;

  @Autowired
  private Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories;

  @Autowired
  @Qualifier("defaultCdcPipelineFactory")
  private CdcPipelineFactory defaultCdcPipelineFactory;

  @Autowired
  @Qualifier("defaultCdcPipelineReaderFactory")
  private CdcPipelineReaderFactory defaultCdcPipelineReaderFactory;

  @Autowired
  private CdcPipelineProperties defaultCdcPipelineProperties;

  @Autowired
  private CdcPipelineReaderProperties defaultCdcPipelineReaderProperties;

  @Autowired
  private BinlogEntryReaderProvider binlogEntryReaderProvider;

  @Autowired
  private DefaultSourceTableNameResolver defaultSourceTableNameResolver;

  @Autowired
  private LeaderSelectorFactory leaderSelectorFactory;

  @Autowired
  private CdcMessageCleanerConfigurator cdcMessageCleanerConfigurator;

  private Map<String, CdcPipelineProperties> pipelineProperties = new HashMap<>();
  private Map<String, CdcPipelineReaderProperties> pipelineReaderProperties = new HashMap<>();

  @Autowired
  private PipelineConfigPropertiesProvider pipelineConfigPropertiesProvider;

  @PostConstruct
  public void initialize() {
    logger.info("Starting unified cdc pipelines");

    pipelineConfigPropertiesProvider.pipelineReaderProperties()
            .ifPresentOrElse(readers -> readers.forEach(this::createCdcPipelineReader), () -> createStartSaveCdcDefaultPipelineReader(defaultCdcPipelineReaderProperties));

    if (dryRunOption) {
      dryRun();
    } else {
      start();
      cdcMessageCleanerConfigurator.startMessageCleaners(pipelineProperties, pipelineReaderProperties);
    }
  }

  @PreDestroy
  public void stop() {
    binlogEntryReaderProvider.stop();

    cdcPipelines.forEach(CdcPipeline::stop);

    cdcMessageCleanerConfigurator.stopMessageCleaners();
  }

  private void start() {
    pipelineConfigPropertiesProvider.pipelineProperties()
            .ifPresentOrElse(pipelines -> pipelines.forEach(this::createStartSaveCdcPipeline), () -> createStartSaveCdcDefaultPipeline(defaultCdcPipelineProperties));

    binlogEntryReaderProvider.start();

    logger.info("Unified cdc pipelines are started");
  }

  private void dryRun() {
    logger.warn("Unified cdc pipelines are not started, 'dry run' option is used");

    List<MySqlBinaryLogClient> clients = binlogEntryReaderProvider
            .getAll()
            .stream()
            .map(BinlogEntryReaderLeadership::getBinlogEntryReader)
            .filter(binlogEntryReader -> binlogEntryReader instanceof MySqlBinaryLogClient)
            .map(binlogEntryReader -> (MySqlBinaryLogClient)binlogEntryReader)
            .collect(Collectors.toList());

    clients
            .forEach(client -> {
              Optional<MySqlBinaryLogClient.MigrationInfo> migrationInfo = client.getMigrationInfo();

              String message = migrationInfo
                      .map(info -> String.format("MySqlBinaryLogClient '%s' received '%s' from the debezium storage, migration should be performed",
                              client.getReaderName(), info.getBinlogFileOffset()))
                      .orElse(String.format("MySqlBinaryLogClient '%s' did not receive offset from the debezium storage, migration should not be performed",
                              client.getReaderName()));

              logger.info(message);
            });

    if (clients.isEmpty()) {
      logger.info("There is no mysql binlog readers, migration information is unavailable.");
    }

    logger.warn("'dry run' option is used, application will be stopped.");
    System.exit(0);
  }

  private void createStartSaveCdcPipeline(String pipeline, CdcPipelineProperties cdcPipelineProperties) {

    if (cdcPipelineProperties.getSourceTableName() == null) {
      cdcPipelineProperties.setSourceTableName(defaultSourceTableNameResolver.resolve(cdcPipelineProperties.getType()));
    }

    pipelineProperties.put(pipeline.toLowerCase(), cdcPipelineProperties);

    CdcPipeline cdcPipeline = createCdcPipeline(cdcPipelineProperties);
    cdcPipeline.start();
    cdcPipelines.add(cdcPipeline);
  }

  private void createStartSaveCdcDefaultPipeline(CdcPipelineProperties cdcDefaultPipelineProperties) {
    cdcDefaultPipelineProperties.validate();
    CdcPipeline cdcPipeline = defaultCdcPipelineFactory.create(cdcDefaultPipelineProperties);
    cdcPipeline.start();
    cdcPipelines.add(cdcPipeline);
  }

  private void createStartSaveCdcDefaultPipelineReader(CdcPipelineReaderProperties cdcDefaultPipelineReaderProperties) {
    cdcDefaultPipelineReaderProperties.validate();

    BinlogEntryReader binlogEntryReader = defaultCdcPipelineReaderFactory.create(cdcDefaultPipelineReaderProperties);

    BinlogEntryReaderLeadership binlogEntryReaderLeadership = new BinlogEntryReaderLeadership(cdcDefaultPipelineReaderProperties.getLeadershipLockPath(),
            leaderSelectorFactory,
            binlogEntryReader);

    binlogEntryReaderProvider.add(cdcDefaultPipelineReaderProperties.getReaderName(), binlogEntryReaderLeadership);
  }

  private CdcPipeline<?> createCdcPipeline(CdcPipelineProperties properties) {

    CdcPipelineFactory<?> cdcPipelineFactory = findCdcPipelineFactory(properties.getType());
    return cdcPipelineFactory.create(properties);
  }

  private void createCdcPipelineReader(String name, CdcPipelineReaderProperties properties) {

    CdcPipelineReaderFactory<? extends CdcPipelineReaderProperties, ? extends BinlogEntryReader> cdcPipelineReaderFactory =
            findCdcPipelineReaderFactory(properties.getType());

    pipelineReaderProperties.put(name.toLowerCase(), properties);

    BinlogEntryReader binlogEntryReader = ((CdcPipelineReaderFactory)cdcPipelineReaderFactory).create(properties);

    BinlogEntryReaderLeadership binlogEntryReaderLeadership = new BinlogEntryReaderLeadership(properties.getLeadershipLockPath(),
            leaderSelectorFactory,
            binlogEntryReader);

    binlogEntryReaderProvider.add(name, binlogEntryReaderLeadership);
  }

  private CdcPipelineFactory<?> findCdcPipelineFactory(String type) {
    return cdcPipelineFactories
            .stream()
            .filter(factory ->  factory.supports(type))
            .findAny()
            .orElseThrow(() ->
                    new RuntimeException(String.format("pipeline factory not found for type %s",
                            type)));
  }

  private CdcPipelineReaderFactory<? extends CdcPipelineReaderProperties, BinlogEntryReader> findCdcPipelineReaderFactory(String type) {
    return cdcPipelineReaderFactories
            .stream()
            .filter(factory ->  factory.supports(type))
            .findAny()
            .orElseThrow(() ->
                    new RuntimeException(String.format("reader factory not found for type %s",
                            type)));
  }
}
