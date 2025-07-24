package io.eventuate.local.unified.cdc.pipeline;

import io.eventuate.local.common.BinlogEntryReader;
import io.eventuate.local.unified.cdc.pipeline.common.PropertyReader;
import io.eventuate.local.unified.cdc.pipeline.common.factory.CdcPipelineReaderFactory;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.CdcPipelineReaderProperties;
import io.eventuate.local.unified.cdc.pipeline.common.properties.RawUnifiedCdcProperties;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PipelineConfigPropertiesProvider {

    private PropertyReader propertyReader = new PropertyReader();

    private RawUnifiedCdcProperties rawUnifiedCdcProperties;
    private Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories;

    public PipelineConfigPropertiesProvider(RawUnifiedCdcProperties rawUnifiedCdcProperties, Collection<CdcPipelineReaderFactory> cdcPipelineReaderFactories) {
        this.rawUnifiedCdcProperties = rawUnifiedCdcProperties;
        this.cdcPipelineReaderFactories = cdcPipelineReaderFactories;
    }

    public Optional<Map<String, CdcPipelineReaderProperties>> pipelineReaderProperties() {
        if (rawUnifiedCdcProperties.isReaderPropertiesDeclared()) {
            return Optional.of(makeFromProperties(rawUnifiedCdcProperties.getReader(), this::createCdcPipelineReaderProperties, CdcPipelineReaderProperties::getReaderName));
        } else
            return Optional.empty();
    }

    public Optional<Map<String, CdcPipelineProperties>> pipelineProperties() {
        if (rawUnifiedCdcProperties.isPipelinePropertiesDeclared()) {
            return Optional.of(makeFromProperties(rawUnifiedCdcProperties.getPipeline(), this::createPipelineProperties, CdcPipelineProperties::getName));
        } else
            return Optional.empty();
    }

    private <T> Map<String, T> makeFromProperties(Map<String, Map<String, Object>> properties, BiFunction<String, Map<String, Object>, T> creator, Function<T, String> nameGetter) {
        return properties.entrySet().stream().map(entry -> creator.apply(entry.getKey(), entry.getValue())).collect(Collectors.toMap(nameGetter, x -> x));
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
                        new RuntimeException("reader factory not found for type %s".formatted(
                                type)));
    }

    private CdcPipelineProperties createPipelineProperties(String name, Map<String, Object> properties) {

        propertyReader.checkForUnknownProperties(properties, CdcPipelineProperties.class);

        CdcPipelineProperties cdcPipelineProperties = propertyReader
                .convertMapToPropertyClass(properties, CdcPipelineProperties.class);

        cdcPipelineProperties.validate();
        cdcPipelineProperties.setName(name);
        return cdcPipelineProperties;
    }


}
