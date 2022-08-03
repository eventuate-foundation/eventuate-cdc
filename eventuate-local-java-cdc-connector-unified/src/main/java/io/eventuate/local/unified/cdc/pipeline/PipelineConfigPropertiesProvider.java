package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class PipelineConfigPropertiesProvider {

    private PropertyReader propertyReader = new PropertyReader();

    private RawUnifiedCdcProperties rawUnifiedCdcProperties;
    private Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories;

    public PipelineConfigPropertiesProvider(RawUnifiedCdcProperties rawUnifiedCdcProperties, Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories) {
        this.rawUnifiedCdcProperties = rawUnifiedCdcProperties;
        this.cdcPipelineReaderFactories = cdcPipelineReaderFactories;
    }

    public Optional<List<CdcPipelineReaderProperties>> pipelineReaderProperties() {
        if (rawUnifiedCdcProperties.isReaderPropertiesDeclared()) {
            return Optional.of(makeFromProperties(rawUnifiedCdcProperties.getReader(), this::createCdcPipelineReaderProperties));
        } else
            return Optional.empty();
    }

    public Optional<List<CdcPipelineProperties>> pipelineProperties() {
        if (rawUnifiedCdcProperties.isPipelinePropertiesDeclared()) {
            return Optional.of(makeFromProperties(rawUnifiedCdcProperties.getPipeline(), this::createPipelineProperties));
        } else
            return Optional.empty();
    }

    private <T> List<T> makeFromProperties(Map<String, Map<String, Object>> properties, BiFunction<String, Map<String, Object>, T> creator) {
        return properties.entrySet().stream().map(entry -> creator.apply(entry.getKey(), entry.getValue())).collect(Collectors.toList());
    }

    private CdcPipelineReaderProperties createCdcPipelineReaderProperties(String name, Map<String, Object> properties) {

        String readerType = getReaderType(name, properties);

        CdcPipelineReaderFactory<? extends CdcPipelineReaderProperties, ? extends BinlogEntryReader> cdcPipelineReaderFactory =
                findCdcPipelineReaderFactory(readerType);

        return makeReaderProperties(name, properties, cdcPipelineReaderFactory.propertyClass());
    }

    private String getReaderType(String name, Map<String, Object> properties) {
        CdcPipelineReaderProperties cdcPipelineReaderProperties = propertyReader
                .convertMapToPropertyClass(properties, CdcPipelineReaderProperties.class);
        cdcPipelineReaderProperties.setReaderName(name);
        cdcPipelineReaderProperties.validate();
        return cdcPipelineReaderProperties.getType();
    }

    private CdcPipelineReaderProperties makeReaderProperties(String name, Map<String, Object> properties, Class<? extends CdcPipelineReaderProperties> propertyClass) {
        propertyReader.checkForUnknownProperties(properties, propertyClass);

        CdcPipelineReaderProperties exactCdcPipelineReaderProperties = propertyReader
                .convertMapToPropertyClass(properties, propertyClass);
        exactCdcPipelineReaderProperties.setReaderName(name);
        exactCdcPipelineReaderProperties.validate();
        return exactCdcPipelineReaderProperties;
    }

    private CdcPipelineReaderFactory<? extends CdcPipelineReaderProperties, BinlogEntryReader> findCdcPipelineReaderFactory(String type) {
        return cdcPipelineReaderFactories
                .stream()
                .filter(factory -> factory.supports(type))
                .findAny()
                .orElseThrow(() ->
                        new RuntimeException(String.format("reader factory not found for type %s",
                                type)));
    }

    private CdcPipelineProperties createPipelineProperties(String name, Map<String, Object> properties) {

        propertyReader.checkForUnknownProperties(properties, CdcPipelineProperties.class);

        CdcPipelineProperties cdcPipelineProperties = propertyReader
                .convertMapToPropertyClass(properties, CdcPipelineProperties.class);

        cdcPipelineProperties.validate();

        return cdcPipelineProperties;
    }


}
