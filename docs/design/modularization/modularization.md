# CDC Service Modularization

This document describes a plan to restructure the eventuate-cdc modules so that readers, data producers, and pipeline orchestration are cleanly separated with well-defined abstraction boundaries.

## Motivation

The current module structure has several problems:

1. **No clear module boundaries.** `connector-common` depends on `data-producer-wrappers`, which pulls in all four message brokers (Kafka, ActiveMQ, RabbitMQ, Redis). `connector-unified` depends on all three reader implementations (mysql-binlog, polling, postgres-wal). There is no separation between readers, producers, and orchestration -- every module transitively depends on everything else.

2. **Abstractions live alongside implementations.** `CdcPipelineReaderFactory` (the interface) lives in `connector-unified` -- the same module that contains all three reader factory implementations. A reader module cannot depend on just the abstraction. Similarly, `DataProducerFactory` and `DataProducer` live in `data-producer-wrappers`, which also contains all broker implementations.

3. **`connector-common` is a kitchen sink.** It mixes reader concerns (DB access, binlog entry handling) with producer concerns (data publishing) and depends on Kafka producers, Kafka consumers, and all three DB drivers.

4. **Hard-wired auto-configuration.** `CdcTramPipelineConfiguration` explicitly `@Import`s all reader configurations. There is no way to exclude a reader without modifying this class.

## Current Module Structure

```
eventuate-cdc-service
  └── eventuate-cdc-connector-autoconfigure
       └── eventuate-tram-cdc-connector ──────────────── messaging libs (Kafka, ActiveMQ, RabbitMQ, Redis)
            └── eventuate-local-java-cdc-connector-unified
                 ├── eventuate-local-java-cdc-connector-mysql-binlog
                 ├── eventuate-local-java-cdc-connector-polling
                 └── eventuate-local-java-cdc-connector-postgres-wal
                      └── eventuate-local-java-cdc-connector-db-log-common
                           └── eventuate-local-java-cdc-connector-common
                                └── eventuate-cdc-data-producer-wrappers ── ALL broker producers
```

### Where Key Abstractions Currently Live

| Abstraction | Current Module | Problem |
|---|---|---|
| `CdcPipelineReaderFactory` | `connector-unified` | Same module as all implementations |
| `CdcPipelineReaderProperties` | `connector-unified` | Same module as all implementations |
| `DataProducerFactory` / `DataProducer` | `data-producer-wrappers` | Same module as all broker wrappers |
| `CdcDataPublisher` | `connector-common` | Forces dependency on `data-producer-wrappers` |
| `BinlogEntryReader` | `connector-common` | Mixed with producer concerns |
| `CdcPipelineFactory` | `connector-unified` | Concrete class, mixed with reader factories |

## Proposed Module Structure

Modules are organized as nested Gradle subprojects grouped by concern.

### Directory Layout

```
eventuate-cdc/
  eventuate-cdc-reader/
    eventuate-cdc-reader-api/
    eventuate-cdc-reader-db-log-common/
    eventuate-cdc-reader-mysql-binlog/
    eventuate-cdc-reader-postgres-wal/
    eventuate-cdc-reader-polling/
  eventuate-cdc-data-producer/
    eventuate-cdc-data-producer-api/
    eventuate-cdc-data-producer-kafka/
    eventuate-cdc-data-producer-activemq/
    eventuate-cdc-data-producer-rabbitmq/
    eventuate-cdc-data-producer-redis/
  eventuate-cdc-pipeline/
  eventuate-cdc-service/
```

### settings.gradle

```groovy
include 'eventuate-cdc-reader:eventuate-cdc-reader-api'
include 'eventuate-cdc-reader:eventuate-cdc-reader-db-log-common'
include 'eventuate-cdc-reader:eventuate-cdc-reader-mysql-binlog'
include 'eventuate-cdc-reader:eventuate-cdc-reader-postgres-wal'
include 'eventuate-cdc-reader:eventuate-cdc-reader-polling'

include 'eventuate-cdc-data-producer:eventuate-cdc-data-producer-api'
include 'eventuate-cdc-data-producer:eventuate-cdc-data-producer-kafka'
include 'eventuate-cdc-data-producer:eventuate-cdc-data-producer-activemq'
include 'eventuate-cdc-data-producer:eventuate-cdc-data-producer-rabbitmq'
include 'eventuate-cdc-data-producer:eventuate-cdc-data-producer-redis'

include 'eventuate-cdc-pipeline'
include 'eventuate-cdc-service'
```

### Reader Modules (`eventuate-cdc-reader/`)

#### `eventuate-cdc-reader-api`

Extracted from `connector-common` and `connector-unified`. Contains only reader abstractions:

- `BinlogEntryReader` (interface)
- `BinlogEntryReaderLeadership`
- `BinlogEntryReaderProvider`
- `CdcPipelineReaderFactory` (interface)
- `CdcPipelineReaderProperties` (base class)
- `BinlogEntryHandler`, `BinLogEvent`, related interfaces

Dependencies: minimal (leadership API, Spring framework).

#### `eventuate-cdc-reader-db-log-common`

Refactored from `connector-db-log-common`. Shared utilities for log-based readers (offset store, etc.).

Dependencies: `eventuate-cdc-reader-api`.

#### `eventuate-cdc-reader-mysql-binlog`

Refactored from `connector-mysql-binlog` plus `connector-unified/dblog/mysqlbinlog`.

Contains:
- `MySqlBinaryLogClient`
- `MySqlBinlogCdcPipelineReaderFactory`
- `MySqlBinlogCdcPipelineReaderProperties`
- `MySqlBinlogCdcPipelineReaderConfiguration` (auto-configuration)

Dependencies: `eventuate-cdc-reader-api`, `eventuate-cdc-reader-db-log-common`, mysql-binlog-connector-java.

#### `eventuate-cdc-reader-postgres-wal`

Refactored from `connector-postgres-wal` plus `connector-unified/dblog/postgreswal`.

Contains:
- `PostgresWalClient`
- `PostgresWalCdcPipelineReaderFactory`
- `PostgresWalCdcPipelineReaderProperties`
- `PostgresWalCdcPipelineReaderConfiguration` (auto-configuration)

Dependencies: `eventuate-cdc-reader-api`, `eventuate-cdc-reader-db-log-common`, postgresql driver.

#### `eventuate-cdc-reader-polling`

Refactored from `connector-polling` plus `connector-unified/polling`.

Contains:
- `PollingDao`
- `PollingCdcPipelineReaderFactory`
- `PollingPipelineReaderProperties`
- `PollingCdcPipelineReaderConfiguration` (auto-configuration)

Dependencies: `eventuate-cdc-reader-api`, Spring JDBC.

### Data Producer Modules (`eventuate-cdc-data-producer/`)

#### `eventuate-cdc-data-producer-api`

Extracted from `data-producer-wrappers` and `connector-common`. Contains only producer abstractions:

- `DataProducer` (interface)
- `DataProducerFactory` (interface)
- `CdcDataPublisher`
- `PublishingStrategy` (interface)
- `PublishingFilter`

Dependencies: minimal (micrometer for metrics, Spring framework).

#### `eventuate-cdc-data-producer-kafka`

Extracted from `data-producer-wrappers`.

Contains:
- `EventuateKafkaDataProducerWrapper`
- Kafka-specific `DataProducerFactory` configuration (auto-configuration)

Dependencies: `eventuate-cdc-data-producer-api`, eventuate-messaging-kafka-spring-producer.

#### `eventuate-cdc-data-producer-activemq`

Contains:
- `EventuateActiveMQDataProducerWrapper`
- ActiveMQ-specific `DataProducerFactory` configuration (auto-configuration)

Dependencies: `eventuate-cdc-data-producer-api`, eventuate-messaging-activemq-spring-producer.

#### `eventuate-cdc-data-producer-rabbitmq`

Contains:
- `EventuateRabbitMQDataProducerWrapper`
- RabbitMQ-specific `DataProducerFactory` configuration (auto-configuration)

Dependencies: `eventuate-cdc-data-producer-api`, eventuate-messaging-rabbitmq-spring-producer.

#### `eventuate-cdc-data-producer-redis`

Contains:
- `EventuateRedisDataProducerWrapper`
- Redis-specific `DataProducerFactory` configuration (auto-configuration)

Dependencies: `eventuate-cdc-data-producer-api`, eventuate-messaging-redis-spring-producer.

### Pipeline Module

#### `eventuate-cdc-pipeline`

Refactored from `connector-unified` (orchestration only) and parts of `tram-cdc-connector`.

Contains:
- `UnifiedCdcConfigurator`
- `PipelineConfigPropertiesProvider`
- `CdcPipeline`
- `CdcPipelineFactory`
- `CdcPipelineProperties`
- `CdcMessageCleanerConfigurator`
- `DefaultSourceTableNameResolver`

Dependencies: `eventuate-cdc-reader-api`, `eventuate-cdc-data-producer-api`. **No implementation dependencies.**

### Assembly Module

#### `eventuate-cdc-service`

The deployable Spring Boot application. Includes all reader and data producer implementations:

```gradle
dependencies {
    implementation project(":eventuate-cdc-pipeline")

    // Readers
    implementation project(":eventuate-cdc-reader:eventuate-cdc-reader-mysql-binlog")
    implementation project(":eventuate-cdc-reader:eventuate-cdc-reader-postgres-wal")
    implementation project(":eventuate-cdc-reader:eventuate-cdc-reader-polling")

    // Data producers
    implementation project(":eventuate-cdc-data-producer:eventuate-cdc-data-producer-kafka")
    implementation project(":eventuate-cdc-data-producer:eventuate-cdc-data-producer-activemq")
    implementation project(":eventuate-cdc-data-producer:eventuate-cdc-data-producer-rabbitmq")
    implementation project(":eventuate-cdc-data-producer:eventuate-cdc-data-producer-redis")
}
```

## Proposed Dependency Graph

```
eventuate-cdc-service
  ├── eventuate-cdc-pipeline
  │    ├── eventuate-cdc-reader/eventuate-cdc-reader-api
  │    └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-api
  │
  ├── eventuate-cdc-reader/eventuate-cdc-reader-mysql-binlog
  │    ├── eventuate-cdc-reader/eventuate-cdc-reader-api
  │    └── eventuate-cdc-reader/eventuate-cdc-reader-db-log-common
  │         └── eventuate-cdc-reader/eventuate-cdc-reader-api
  │
  ├── eventuate-cdc-reader/eventuate-cdc-reader-postgres-wal
  │    ├── eventuate-cdc-reader/eventuate-cdc-reader-api
  │    └── eventuate-cdc-reader/eventuate-cdc-reader-db-log-common
  │
  ├── eventuate-cdc-reader/eventuate-cdc-reader-polling
  │    └── eventuate-cdc-reader/eventuate-cdc-reader-api
  │
  ├── eventuate-cdc-data-producer/eventuate-cdc-data-producer-kafka
  │    └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-api
  │
  ├── eventuate-cdc-data-producer/eventuate-cdc-data-producer-activemq
  │    └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-api
  │
  ├── eventuate-cdc-data-producer/eventuate-cdc-data-producer-rabbitmq
  │    └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-api
  │
  └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-redis
       └── eventuate-cdc-data-producer/eventuate-cdc-data-producer-api
```

## Auto-Configuration Strategy

Replace the current hard-wired `@Import` in `CdcTramPipelineConfiguration` with per-module auto-configuration.

Each reader and data producer module registers its own configuration class in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Spring Boot discovers them automatically when the module is on the classpath.

**Before** (`CdcTramPipelineConfiguration`):
```java
@Import({MySqlBinlogCdcPipelineReaderConfiguration.class,
         PollingCdcPipelineReaderConfiguration.class,
         PostgresWalCdcPipelineReaderConfiguration.class})
```

**After** (each reader module's `AutoConfiguration.imports`):
```
# In eventuate-cdc-reader/eventuate-cdc-reader-mysql-binlog
io.eventuate.cdc.reader.mysqlbinlog.MySqlBinlogCdcPipelineReaderConfiguration
```

`UnifiedCdcConfigurator` continues to use `@Autowired Collection<CdcPipelineReaderFactory>` -- Spring discovers whatever factories are on the classpath.

## CdcPipelineFactory: Class vs Interface

`CdcPipelineFactory` is currently a concrete class parameterized with different `BinlogEntryToEventConverterFactory` and `CdcDataPublisher` instances for "eventuate-tram" vs "eventuate-local" pipeline types. This design is fine -- it is a configurable class, not a type hierarchy. It stays as a single class in the `pipeline` module, instantiated with different converters by different `@Configuration` classes.

The tram-specific and eventuate-local-specific converter configurations move into the pipeline module (or a thin `eventuate-cdc-pipeline-tram` / `eventuate-cdc-pipeline-eventuate-local` module if full separation is desired).

## CI Test Matrix Reduction

### Current CI E2E Test Matrix

The CI pipeline (`.circleci/config.yml`) tests three layers:

**Layer 1: Reader unit/integration tests** (per reader, no broker):

| Job | Reader | Database |
|---|---|---|
| `build-and-test-mysql8` | binlog | MySQL 8 |
| `build-and-test-postgres-wal` | WAL | PostgreSQL |
| `build-and-test-postgres-polling` | polling | PostgreSQL |
| `build-and-test-mssql-polling` | polling | MSSQL |

**Layer 2: E2E tests** (reader x broker, full stack in Docker):

Eventuate Local tests only use Kafka. Tram tests use all brokers:

| Job | Reader | Kafka | ActiveMQ | RabbitMQ | Redis |
|---|---|---|---|---|---|
| `tram-cdc-mysql8-binlog` | MySQL binlog | yes | yes | yes | yes |
| `tram-cdc-postgres-wal` | Postgres WAL | yes | yes | yes | yes |
| `tram-cdc-postgres-polling` | Postgres polling | yes | yes | yes | yes |
| `tram-cdc-mssql-polling` | MSSQL polling | yes | no | no | no |

**Layer 3: Unified + multi-arch** (all readers in one service, plus architecture verification).

### Why the Full Matrix is Unnecessary

The data flow through the system is:

```
Reader → BinlogEntryHandler → CdcDataPublisher.sendMessage() → DataProducer → Broker
```

The reader and broker are fully decoupled by `CdcDataPublisher.sendMessage()`. The reader never sees the broker, and the broker never sees the reader. The data passed through this interface is identical regardless of which reader or broker is used.

A bug in the Redis producer cannot be triggered by switching from MySQL binlog to Postgres WAL. A bug in the WAL reader cannot be triggered by switching from Kafka to RabbitMQ. The full matrix can only catch bugs in an interaction that the architecture prevents.

The Eventuate Local tests and the `database-id-jsonschema` tests already follow this principle -- they only test with Kafka.

### Proposed Reduction

Test each dimension independently:

- **Reader tests** (Layer 1): Keep as-is -- 4 jobs testing each reader against its database.
- **Broker tests**: Pick one reader (e.g., MySQL binlog) and test all 4 brokers -- **4 jobs**.
- **Other readers**: Test with Kafka only -- **3 jobs** (postgres-wal, postgres-polling, mssql-polling).
- **Unified + multi-arch**: Keep as-is.

This drops the Tram E2E from **13 reader x broker combinations to 7**, cutting ~6 CI jobs with no loss of effective coverage. The modularization reinforces this: once readers and data producers live in separate modules with no compile-time dependency on each other, the impossibility of cross-dimension bugs is enforced by the build, not just by convention.

## Migration Order

### Phase 1: Extract API Modules

1. Create `eventuate-cdc-reader/eventuate-cdc-reader-api` with reader interfaces extracted from `connector-common` and `connector-unified`.
2. Create `eventuate-cdc-data-producer/eventuate-cdc-data-producer-api` with producer interfaces extracted from `data-producer-wrappers` and `connector-common`.
3. Update existing modules to depend on the new API modules instead of the kitchen-sink modules.
4. Verify all tests pass.

### Phase 2: Split Data Producer Implementations

5. Create per-broker data producer modules under `eventuate-cdc-data-producer/`, each depending on the API subproject.
6. Move wrapper classes from `data-producer-wrappers` into the per-broker modules.
7. Add auto-configuration to each broker module.
8. Verify all tests pass.

### Phase 3: Consolidate Reader Modules

9. Move reader factory classes from `connector-unified` into their respective modules under `eventuate-cdc-reader/`.
10. Move reader configuration classes into their respective reader modules.
11. Add auto-configuration to each reader module.
12. Verify all tests pass.

### Phase 4: Create Pipeline Module

13. Create `eventuate-cdc-pipeline` with orchestration classes from `connector-unified`.
14. Remove hard `@Import` from `CdcTramPipelineConfiguration`; rely on per-module auto-configuration.
15. Slim down or remove `connector-unified` and `connector-common`.
16. Verify all tests pass.

### Phase 5: Clean Up

17. Remove empty or vestigial modules.
18. Update `eventuate-cdc-service` to depend on the new module set.
19. Update documentation.

## Benefits

- **Clear boundaries.** Each module has a single responsibility -- a reader reads from a database, a data producer sends to a broker, the pipeline wires them together. Dependencies flow in one direction through well-defined API modules.
- **Easier to understand.** A developer working on the MySQL binlog reader only needs to understand `reader-api` and the MySQL-specific module, not the entire codebase.
- **Independent evolution.** Add a new reader or broker by creating a new module -- no changes to existing code.
- **Cleaner testing.** Test a reader without broker dependencies on the classpath and vice versa.
- **Simple orchestrator.** `UnifiedCdcConfigurator` depends only on abstractions, not implementations.
