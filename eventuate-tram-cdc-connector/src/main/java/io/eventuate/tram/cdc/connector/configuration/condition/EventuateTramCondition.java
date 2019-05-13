package io.eventuate.tram.cdc.connector.configuration.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class EventuateTramCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String type = context.getEnvironment().resolvePlaceholders("${eventuate.cdc.type:}");
    return type.isEmpty() || "EventuateTram".equals(type);
  }
}
