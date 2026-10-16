# Contributor guide

## Project purpose and capabilities

The project's purpose is to provide a legacy-state migration path for Apache Flink jobs using Avro. With Flink's regular Avro serializer, a schema change such as adding or removing a field can require migration and rewrite of the entire state, potentially taking hours for large state. This serializer instead resolves each record's writer schema at read time and projects it to the current reader schema, avoiding an eager full-state rewrite for compatible changes. This follows the same broad pattern used by data lakes, where old records are projected to a reader schema rather than rewriting all historical data after every schema change.

The implementation provides an Apache Flink `TypeSerializer` for Avro `GenericRecord` and generated `SpecificRecord` state using Confluent Schema Registry framing. Every newly written non-null record carries its writer schema ID in its bytes. On restore, Confluent resolves the writer schema and Avro projects the value onto the reader schema configured by the current job.

The serializer also supports a transition from Flink's regular Avro serializer: it reads the old `AvroSerializerSnapshot` to obtain the single legacy writer schema, reads those raw Avro values in place, and writes subsequent updates in Confluent format. Legacy and Confluent values may coexist in state. Do not claim support for multiple legacy writer schemas from one previous snapshot or for arbitrary serializer migrations.

The serializer snapshot version is 1. It records the record kind and, for SpecificRecord, the record class name, plus any legacy raw-Avro schema and the last completed force-migration nonce. It does not persist a GenericRecord schema: each Confluent-framed value resolves its writer schema from its own schema ID. Subject and Schema Registry client configuration are current-runtime settings, not snapshot data. `restoreSerializer()` eagerly creates a read-only historical serializer: GenericRecord reads resolve each stored event's writer schema and read using that schema, while SpecificRecord reads load the named class from the current user-code classloader and use that class's current schema. Registry client settings for this historical reader come from Flink global configuration.

For a newly selected force-migration nonce, the current writer rewrites state. SpecificRecord decoding already targets the current runtime class/schema. GenericRecord decoding uses its historical schema, so while migration is pending the current serializer checks the schema only on writes and projects old-schema records to the current schema before writing. This extra comparison/projection path is limited to GenericRecord forced migration.

When there is no legacy raw-Avro schema to carry forward, serializer snapshot changes are Flink-compatible-as-is; when a legacy schema is present it is preserved via a reconfigured serializer. Actual value readability depends on Avro writer/reader schema compatibility. Registry operations use Confluent's Schema Registry client and may perform network calls during state serialization/deserialization.

## Build and test

Use the Gradle Wrapper:

```powershell
./gradlew check
./gradlew check -PjavaVersion=21 '-PflinkVersion=2.2.1'
```

Quote `-PflinkVersion=...` in PowerShell. Defaults are Java 17 and Flink 2.3.0. CI tests all Java 17/21 and Flink 2.2.1/2.3.0 combinations.

Tests include unit coverage for framing, nulls, copies, legacy reads, snapshot compatibility, nested Avro schema projection and forced migration, and authenticated Schema Registry client requests using environment- and file-based secrets. RocksDB-backed MiniCluster savepoint tests cover GenericRecord and generated SpecificRecord restore. GenericRecord has no schema in its Java class: use `GenericRecordAvroTypeInfo(schema)` when testing Flink's regular Avro serializer. Keep the test's Kryo/generic fallback disabled so a missing Avro type-information setup fails rather than silently exercising Kryo. Do not replace inferred SpecificRecord Avro type information or schema-aware GenericRecord type information with an explicit `AvroSerializer` in fallback/migration tests; those tests verify Flink's default behavior.

Keep OS detection and platform-specific test temporary paths in Gradle's `Test` task configuration. Java tests should read the configured paths from system properties and should not branch on `os.name`. Set test environment variables such as `FLINK_CONF_DIR` in Gradle rather than changing the process environment from Java tests.

## Implementation rules

- Compile against Flink as `compileOnly`; the consuming Flink distribution provides it at runtime.
- Keep the Confluent Schema Registry client dependency `compileOnly`; consuming jobs choose a compatible client version at runtime. Do not add a dependency on Confluent Kafka Avro serializer/deserializer classes. Avro is part of the published API.
- Use the configured subject literally for schema registration and ID lookup. Schema IDs, not subjects, select writer schemas during reads.
- The public factory map is named `serializerConfig`. Retain only `schema.registry.url`, `auto.register.schemas`, `normalize.schemas`, `max.schemas.per.subject`, and optional `force.migration.nonce`; log and ignore other map keys. `force.migration.nonce` can also be set in Flink global config as `schema.resolving.avro.force.migration.nonce`; the map value takes priority. For duplicate Registry client settings, `serializerConfig` takes priority and Flink global config under `schema.resolving.avro.*` supplies missing values. Keep credentials out of the serialized config map.
- An integer `force.migration.nonce` requests compatible-after-migration for supported Confluent and regular Flink Avro state when it differs from the nonce recorded in the previous snapshot. Persist the new nonce in snapshot version 1 so a restart with the same nonce does not repeat the rewrite. A different nonce requests another migration.
- Resolve runtime Registry client settings from `serializerConfig` first, then fill missing settings from Flink global config under `schema.resolving.avro.*`. Secret references use `schema.resolving.avro.secret.<client-property>.env` or `.file`, exactly one source per client property. Read secrets at client creation, trim file contents, and never persist or log resolved values.
- Implement value reuse by passing the supplied record to Avro `DatumReader.read(reuse, decoder)` for both Confluent-framed values and legacy raw Avro values.
- Keep the serializer snapshot version at 1. Review snapshot payload changes for compatibility; this project has not been deployed, so the v1 payload may evolve before release. Any wire-format change requires an explicit snapshot/version compatibility review.
- Never serialize Schema Registry credentials into a serializer or snapshot. Reject secret-bearing config keys and keep runtime credentials external to the serializer's serializable config map.
- Keep serializer and snapshot behavior deterministic and test binary compatibility, null handling, schema projection, copies, legacy raw-Avro bridging, and savepoint restoration.
- Do not commit credentials or signing keys.

## Publishing

Coordinates are `io.github.gliter:schema-resolving-avro-serializer`, with artifact versions in the format `major.minor.patch-<Flink version>`. Use `centralBundle` to create the signed archive consumed by the tag-driven release workflow. Release tags contain only the library version, such as `v1.2.3`; the workflow publishes one artifact for each supported Flink version.
