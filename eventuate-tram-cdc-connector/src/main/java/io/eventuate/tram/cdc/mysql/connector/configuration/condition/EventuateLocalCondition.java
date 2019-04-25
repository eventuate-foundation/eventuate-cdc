package io.eventuate.tram.cdc.mysql.connector.configuration.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class EventuateLocalCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String type = context.getEnvironment().resolvePlaceholders("${eventuate.cdc.type:}");
    return "EventuateLocal".equals(type);
  }
}
