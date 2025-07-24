package io.eventuate.tram.cdc.connector.configuration.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Profiles;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class KafkaCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return !context.getEnvironment().acceptsProfiles(Profiles.of("ActiveMQ")) &&
            !context.getEnvironment().acceptsProfiles(Profiles.of("RabbitMQ")) &&
            !context.getEnvironment().acceptsProfiles(Profiles.of("Redis"));
  }
}
