package io.eventuate.cdc.testcontainers;

import io.eventuate.common.testcontainers.ContainerUtil;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateCdcContainer extends GenericContainer<EventuateCdcContainer> implements PropertyProvidingContainer {

    public EventuateCdcContainer() {
        super(ContainerUtil.findImage("eventuateio/eventuate-cdc-service", "eventuate.cdc.version.properties"));
        withConfiguration();
    }

    public EventuateCdcContainer(Path path) {
        super(new ImageFromDockerfile().withDockerfile(path));
        withConfiguration();
    }

    private void withConfiguration() {
    }

    @Override
    public void registerProperties(BiConsumer<String, Supplier<Object>> registry) {

    }
}
