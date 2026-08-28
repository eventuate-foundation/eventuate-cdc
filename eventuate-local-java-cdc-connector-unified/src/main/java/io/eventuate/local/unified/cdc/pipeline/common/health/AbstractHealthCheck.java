package io.eventuate.local.unified.cdc.pipeline.common.health;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.List;

public abstract class AbstractHealthCheck implements HealthIndicator {


  @Override
  public Health health() {
    HealthBuilder builder = new HealthBuilder();
    determineHealth(builder);
    return builder.build();
  }

  protected abstract void determineHealth(HealthBuilder builder);

}
