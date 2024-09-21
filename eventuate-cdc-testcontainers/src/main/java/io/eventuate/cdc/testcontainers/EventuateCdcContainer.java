package io.eventuate.cdc.testcontainers;

import io.eventuate.common.testcontainers.ContainerUtil;
import io.eventuate.common.testcontainers.EventuateDatabaseContainer;
import io.eventuate.common.testcontainers.EventuateGenericContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaCluster;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaNativeCluster;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaNativeContainer;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateCdcContainer extends EventuateGenericContainer<EventuateCdcContainer> implements PropertyProvidingContainer {

    private int pipelineIdx;

    public EventuateCdcContainer() {
        super(ContainerUtil.findImage("eventuateio/eventuate-cdc-service", "eventuate.cdc.version.properties"));
        withConfiguration();
    }

    public EventuateCdcContainer(Path path) {
        super(new ImageFromDockerfile().withDockerfile(path));
        withConfiguration();
    }

    @NotNull
    static EventuateCdcContainer makeFromDockerfile() {
        return new EventuateCdcContainer(FileSystems.getDefault().getPath("../eventuate-cdc-service/Dockerfile"));
    }

    private void withConfiguration() {
        waitingFor(Wait.forHealthcheck());
    }

    @Override
    public void registerProperties(BiConsumer<String, Supplier<Object>> registry) {

    }

    public EventuateCdcContainer withKafkaCluster(EventuateKafkaCluster eventuateKafkaCluster) {
        withNetwork(eventuateKafkaCluster.network);
        withEnv("EVENTUATELOCAL_ZOOKEEPER_CONNECTION_STRING", eventuateKafkaCluster.zookeeper.getConnectionString());
        withEnv("EVENTUATELOCAL_KAFKA_BOOTSTRAP_SERVERS", eventuateKafkaCluster.kafka.getConnectionString());
        return this;
    }

    public EventuateCdcContainer withKafkaCluster(EventuateKafkaNativeCluster eventuateKafkaCluster) {
        withNetwork(eventuateKafkaCluster.network);
        withKafkaLeadership();
        withEnv("EVENTUATELOCAL_KAFKA_BOOTSTRAP_SERVERS", eventuateKafkaCluster.kafka.getBootstrapServersForContainer());
        return this;
    }

    public EventuateCdcContainer withKafka(Network network, EventuateKafkaContainer kafka) {
        withNetwork(network);
        dependsOn(kafka);
        withKafkaLeadership();
        withEnv("EVENTUATELOCAL_KAFKA_BOOTSTRAP_SERVERS", kafka.getConnectionString());
        return this;
    }

    public EventuateCdcContainer withKafka(EventuateKafkaNativeContainer kafka) {
        withNetwork(kafka.getNetwork());
        dependsOn(kafka);
        withEnv("EVENTUATELOCAL_KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServersForContainer());
        return this;
    }
    public EventuateCdcContainer withTramPipeline(EventuateDatabaseContainer<?> database) {

        dependsOn(database);

        newPipeline();

        withEnv("EVENTUATE_CDC_READER_READERX_DATASOURCEURL", database.getJdbcUrl());
        withEnv("EVENTUATE_CDC_READER_READERX_DATASOURCEUSERNAME", database.getAdminCredentials().userName);
        withEnv("EVENTUATE_CDC_READER_READERX_DATASOURCEPASSWORD", database.getAdminCredentials().password);
        withEnv("EVENTUATE_CDC_READER_READERX_MONITORINGSCHEMA", database.getMonitoringSchema());
        withEnv("EVENTUATE_CDC_READER_READERX_LEADERSHIPLOCKPATH", () -> String.format("/eventuate/cdc/leader/%s", database.getContainerId()));
        withEnv("EVENTUATE_CDC_READER_READERX_OFFSETSTORAGETOPICNAME", "db.history.common");
        withEnv("EVENTUATE_CDC_READER_READERX_DATASOURCEDRIVERCLASSNAME", database.getDriverClassName());
        withEnv("EVENTUATE_CDC_READER_READERX_OUTBOXID", Integer.toString(pipelineIdx));

        String cdcReaderType = database.getCdcReaderType();
        withEnv("EVENTUATE_CDC_READER_READERX_TYPE", cdcReaderType);

        if (cdcReaderType.equals("mysql-binlog")) {
            withEnv("EVENTUATE_CDC_READER_READERX_CDCDBUSERNAME", database.getAdminCredentials().userName);
            withEnv("EVENTUATE_CDC_READER_READERX_CDCDBPASSWORD", database.getAdminCredentials().password);
            withEnv("EVENTUATE_CDC_READER_READERX_READOLDDEBEZIUMDBOFFSETSTORAGETOPIC", "false");
            withEnv("EVENTUATE_CDC_READER_READERX_MYSQLBINLOGCLIENTUNIQUEID", Integer.toString(pipelineIdx));
            withEnv("EVENTUATE_CDC_READER_READERX_OFFSETSTOREKEY", database::getContainerId);
        }

        withEnv("EVENTUATE_CDC_PIPELINE_PIPELINEX_TYPE", "eventuate-tram");
        withEnv("EVENTUATE_CDC_PIPELINE_PIPELINEX_READER", "reader" + pipelineIdx);
        withEnv("EVENTUATE_CDC_PIPELINE_PIPELINEX_EVENTUATEDATABASESCHEMA", database.getEventuateDatabaseSchema());
        return this;
    }

    private void newPipeline() {
        pipelineIdx++;
    }

    @Override
    public EventuateCdcContainer withEnv(String name, String value) {
        return super.withEnv(replaceName(name), value);
    }

    @Override
    public EventuateCdcContainer withEnv(String name, Supplier<String> valueSupplier) {
        return super.withEnv(replaceName(name), valueSupplier);
    }

    @NotNull
    private String replaceName(String name) {
        return name.replace("_READERX_", String.format("_READER%s_", pipelineIdx))
            .replace("_PIPELINEX_", String.format("_PIPELINE%s_", pipelineIdx));
    }

    @Override
    protected int getPort() {
        return 8080;
    }

    public EventuateCdcContainer withKafkaLeadership() {
        withEnv("SPRING_PROFILES_ACTIVE", "KafkaLeadership");
        return this;
    }

}
