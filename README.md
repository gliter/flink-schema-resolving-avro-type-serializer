# Schema-Resolving Avro Serializer for Flink

=== THIS IS WORK IN PROGRESS ==

[![Build](https://github.com/gliter/flink-confluent-avro-type-serializer/actions/workflows/build.yml/badge.svg)](https://github.com/gliter/flink-confluent-avro-type-serializer/actions/workflows/build.yml)

Need to add an Avro field to a Flink job? A regular Flink Avro serializer can require a full state rewrite after a schema change. For large state, that can take hours.

When you add a field to a data lake schema, you do not rewrite every old file. When you add a field to a Kafka event schema, you do not rewrite every old message. So why should Flink need to rewrite all state just because you added a field?

Schema-Resolving Avro Serializer is a Flink `TypeSerializer` for Avro schema evolution. It reads each value with the schema that wrote it. It then maps the value to the current schema when Flink reads state. This is lazy, read-time migration. For compatible schema changes, the job does not need to rewrite all state first.

The serializer uses Confluent Schema Registry to store and find schemas. It supports generated Avro `SpecificRecord` classes and schema-based `GenericRecord` values. You can also use it with state that a regular Flink Avro serializer wrote before.

If you are looking for a Flink Avro serializer, Avro schema evolution, lazy state migration, or a way to avoid a full Flink state rewrite after adding a field, this project may help.

## Capabilities

- Read compatible Avro schema changes without a full state rewrite.
- Use generated `SpecificRecord` classes or `GenericRecord` values.
- Find each value's writer schema by its Schema Registry ID.
- Use this serializer as a drop-in replacement for Flink's regular Avro serializer on existing state.
- Force a full migration to the current schema on demand.

## Quick start

### Requirements

- Java 17 or Java 21.
- Apache Flink 2.2.1 or 2.3.0.
- The Flink job needs `flink-avro` and the Confluent Schema Registry client at runtime.
- A Schema Registry that stores the Avro schemas used by the job.

Add the library and runtime dependencies. Use versions that match your Flink setup.

```kotlin
dependencies {
    implementation("io.github.gliter:schema-resolving-avro-serializer:0.1.0-2.3.0")
    compileOnly("org.apache.flink:flink-streaming-java:2.3.0")
    compileOnly("org.apache.flink:flink-avro:2.3.0")
    runtimeOnly("io.confluent:kafka-schema-registry-client:8.1.1")
}
```

Set the Registry URL in Flink's global configuration. You can also set `schema.registry.url` in the serializer config map. If both settings exist, the map value takes priority. Flink global config supplies the URL when the map does not set it.

```yaml
schema.resolving.avro.schema.registry.url: https://schema-registry.example
```

### SpecificRecord

Use the generated record class to create the serializer. Pass the Schema Registry subject and any serializer options.

```java
Map<String, Object> serializerConfig = Map.of(
    "auto.register.schemas", true);

SchemaResolvingAvroSerializer<Customer> serializer =
    SchemaResolvingAvroSerializer.forSpecific(
        "customer-state-subject", Customer.class, serializerConfig);

ValueStateDescriptor<Customer> state =
    new ValueStateDescriptor<>("customer", serializer);
```

The subject is used when the serializer writes a schema. The subject is not needed to read a value. The value's schema ID selects its writer schema.

### GenericRecord

`GenericRecord` has no schema in its Java class. Give the serializer the reader schema for the current job.

```java
Schema currentSchema = new Schema.Parser().parse(schemaJson);

SchemaResolvingAvroSerializer<GenericRecord> serializer =
    SchemaResolvingAvroSerializer.forGeneric(
        "customer-state-subject", currentSchema, serializerConfig);

ValueStateDescriptor<GenericRecord> state =
    new ValueStateDescriptor<>("customer", serializer);
```

## How schema resolution works

Each value carries the identity of the schema that encoded it. The serializer does not rely on the latest schema for a subject when it reads old state. This matters because one state may contain records written at different times, under different schema versions.

### When Flink writes a value

For a non-null record, the serializer gets the record's Avro schema and asks Schema Registry for its ID under the configured subject. Depending on `auto.register.schemas`, it registers the schema or requires it to exist already. It then writes a serializer header, the schema ID, and the Avro binary datum. The ID points to the exact writer schema for that value. Null values are marked as null and have no schema ID or datum.

The subject controls schema registration and lookup during writes. It is not used to choose a schema when reading. This means that changing the configured subject does not change how existing values are decoded, as long as their schema IDs can still be resolved by the Registry.

### When Flink reads a value

The serializer first checks its framing marker. For a schema-resolving value, it reads the schema ID and asks Schema Registry for the corresponding writer schema. It then decodes the Avro datum with both schemas:

- The writer schema describes the fields and binary layout in the stored bytes.
- The reader schema describes the record shape expected by the running job.

Avro's resolving decoder matches fields by name and applies Avro's schema-resolution rules while it reads. For a `SpecificRecord`, the reader schema comes from the current generated class. For a `GenericRecord`, it is the schema supplied when the serializer is created. The serializer passes Flink's reuse object to Avro when one is available, so Avro can populate an existing record instead of always allocating a new one.

For example, version 1 may store `name`. Version 2 may keep `name` and add `tier` with a default such as `"standard"`. When the job reads a version-1 value, Avro reads the bytes with the version-1 writer schema and supplies the default required by the version-2 reader schema. The stored bytes do not need to be changed first. If a later job reads a version-2 value, its own schema ID selects version 2's writer schema.

This is lazy schema resolution, not a background rewrite. Old values keep their original bytes until the job updates them or a forced migration is requested. Compatibility still depends on Avro's rules: for example, a reader schema that adds a field needs a default for data written before that field existed. An incompatible change can fail when the value is read; this serializer does not make incompatible schemas compatible by itself.

The Schema Registry client may cache schemas, but a cache miss can require a Registry request during state access. Keep the Registry reachable by the Flink processes that serialize and deserialize state.

## Use with existing Flink Avro state

The examples below show the change from a regular Flink Avro serializer to this serializer. Keep the state descriptor name and operator UID unchanged. Flink uses them to match restored state to the state from the previous job. During restore, Flink checks the previous serializer snapshot. That snapshot provides the schema needed to read old raw-Avro values.

For a generated `SpecificRecord`, the old job can use Flink's inferred Avro type information:

```java
ValueStateDescriptor<Customer> customerState =
    new ValueStateDescriptor<>("customer", Customer.class);
```

In the new job, replace the inferred serializer with `SchemaResolvingAvroSerializer`. Use the current generated class, which supplies the reader schema for restored values:

```java
SchemaResolvingAvroSerializer<Customer> serializer =
    SchemaResolvingAvroSerializer.forSpecific(
        "customer-state-subject", Customer.class, serializerConfig);
ValueStateDescriptor<Customer> customerState =
    new ValueStateDescriptor<>("customer", serializer);
```

The descriptor keeps the state name `customer`. Keep the operator UID from the old job as well; the UID is set on the operator that owns this state, not on the descriptor.

For `GenericRecord`, Flink cannot infer the old schema from `GenericRecord.class`. The old job must have used schema-aware type information, such as `GenericRecordAvroTypeInfo`:

```java
ValueStateDescriptor<GenericRecord> customerState =
    new ValueStateDescriptor<>("customer", new GenericRecordAvroTypeInfo(oldSchema));
```

In the new job, pass the current reader schema to the schema-resolving serializer and keep the state name unchanged:

```java
SchemaResolvingAvroSerializer<GenericRecord> serializer =
    SchemaResolvingAvroSerializer.forGeneric(
        "customer-state-subject", currentSchema, serializerConfig);
ValueStateDescriptor<GenericRecord> customerState =
    new ValueStateDescriptor<>("customer", serializer);
```

The old snapshot supplies the legacy writer schema for old raw-Avro values. The new serializer uses `currentSchema` to read them. New writes use the schema-resolving format. This is a format transition, not an eager conversion: old and new values can coexist until Flink reads and updates an old value. Values that the job does not touch can remain in raw-Avro format.

## Force a full migration

Lazy schema resolution avoids a full rewrite, but some operational changes may require all stored values to use the current schema and wire format. For example, you may want to finish the transition from regular Flink Avro state or remove old schema versions from active state. In that case, request a full migration by setting `force.migration.nonce` to an integer in `serializerConfig`:

```java
Map<String, Object> migrationConfig = Map.of(
    "auto.register.schemas", true,
    "force.migration.nonce", 1);
```

You can also set the nonce in Flink's global configuration by using the `schema.resolving.avro.` prefix:

```yaml
schema.resolving.avro.force.migration.nonce: 1
```

If both locations set a nonce, the value in `serializerConfig` takes priority.

During a full migration of schema-resolving state, Flink creates the previous serializer from its snapshot to read the old values. That historical serializer does not have the new job's `serializerConfig`. Set the Schema Registry URL in Flink's global configuration so the historical serializer can resolve the schemas for those values:

```yaml
schema.resolving.avro.schema.registry.url: https://schema-registry.example
```

A URL set only in `serializerConfig` is not available to this historical serializer. The current serializer can still use the map value; the global URL is needed by the serializer restored from the previous snapshot.

When the nonce changes, Flink runs a full state migration and writes the state with the current serializer. The serializer saves the new nonce in a later snapshot.

Keep the same integer after the migration. That value tells later restores that this migration request has already completed. Change it to another integer only when you want to request another full migration. If the job stops before it creates a snapshot with the completed nonce, a later restore can request the migration again. A full migration can take significant time and I/O for large state, so leave the option unset for normal lazy schema resolution.

## Configuration and secrets

### Serializer options

| Key | Default | Purpose |
| --- | --- | --- |
| `schema.registry.url` | None | Registry URL. This map value takes priority over Flink global config. |
| `auto.register.schemas` | `true` | Register a schema when the subject does not have it. Set to `false` to require an existing schema. |
| `normalize.schemas` | `false` | Normalize schemas before registration or lookup. |
| `max.schemas.per.subject` | `1000` | Set the Schema Registry client's schema cache size. |
| `force.migration.nonce` | Not set | Request a full migration when the integer changes. Set it in `serializerConfig` or as `schema.resolving.avro.force.migration.nonce` in Flink global config. The map value takes priority. |

The writer uses the subject exactly as you pass it. It does not add `-value` or apply a subject naming strategy. With `auto.register.schemas=true`, the writer can register a schema under that subject. With it set to `false`, the schema must already be registered there. The serializer stores the returned schema ID in each value. Reads use that ID, not the subject, to find the writer schema.

`max.schemas.per.subject` controls the Schema Registry client's local schema cache capacity; it does not limit how many versions the Registry stores. Other `serializerConfig` keys are logged and ignored. Kafka Avro serde options such as `specific.avro.reader` do not apply because this serializer uses Avro's datum readers directly.

### Flink Registry settings

Put Schema Registry client settings in Flink's global configuration when they should be available to the serializer at runtime. The serializer reads keys with the `schema.resolving.avro.` prefix and removes that prefix before passing client settings to the Registry. The migration nonce uses the same prefix but is handled by the serializer, not sent to the Registry. For example:

```yaml
schema.resolving.avro.schema.registry.url: https://schema-registry.example
schema.resolving.avro.basic.auth.credentials.source: USER_INFO
```

For non-secret properties, a value in `serializerConfig` takes priority over the matching Flink global setting. Flink configuration supplies a property when the map does not contain it. Secret references are resolved separately when the Registry client is created; the resolved values are not added to `serializerConfig`.

### Credentials

Keep credentials outside both the serializer config map and the Flink serializer snapshot. For each Schema Registry client property that contains a secret, configure exactly one source: an environment variable or a file. The key uses the `schema.resolving.avro.secret.<client-property>.env` or `.file` form:

```yaml
schema.resolving.avro.secret.schema.registry.basic.auth.user.info.env: SCHEMA_REGISTRY_USER_INFO
# Or use a file:
schema.resolving.avro.secret.schema.registry.basic.auth.user.info.file: /var/run/secrets/registry/user-info
```

The environment variable or file must contain the raw value expected by the named client property. For Basic Auth, `basic.auth.user.info` expects `username:password`, and `basic.auth.credentials.source` must also be set to the appropriate mode, such as `USER_INFO`. The serializer reads the secret when it creates the Registry client and trims whitespace from file contents. It does not log or store the resolved value in the serializer or snapshot. On Kubernetes, make the secret available to both JobManager and TaskManager processes, for example by mounting it as a file or exposing it as an environment variable.

## Limits and operational notes

Schema evolution still follows Avro's writer/reader compatibility rules. A value that cannot be resolved to the current reader schema fails when Flink reads it; the serializer does not repair incompatible schemas. A field added to the reader schema usually needs a default to read values written before that field existed.

The legacy bridge supports one raw-Avro writer schema from the previous Flink serializer snapshot. New schema-resolving values do not have this limit because each value stores a Registry schema ID. Schema IDs must remain resolvable in the Registry used by the running job. The client may serve a schema from its cache, but a cache miss needs a Registry request, so the Flink processes need network access and valid credentials.

The configured subject is used when writing and registering schemas. A read uses the ID stored in the value, so it does not need the subject that was used to write that value. Normal schema resolution does not trigger a state rewrite; changing the force-migration nonce does.

## Development

The default build uses Java 17 and Flink 2.3.0. The build can also target Java 21 and Flink 2.2.1. Run the checks for the defaults with:

```shell
./gradlew check
./gradlew check -PjavaVersion=21 '-PflinkVersion=2.2.1'
```

GitHub Actions runs the test matrix for Java 17 and 21 with Flink 2.2.1 and 2.3.0. Unit tests cover serializer framing, null values, copying, schema compatibility, and legacy raw-Avro reads. RocksDB-backed Flink MiniCluster tests create and restore savepoints for `GenericRecord` and generated `SpecificRecord` values. These tests exercise state restore through Flink, not only direct calls to the serializer.

## Publishing

The Maven Central coordinates are `io.github.gliter:schema-resolving-avro-serializer`. Versions use the format `major.minor.patch-<Flink version>`, which makes the target Flink line visible in the published version. The default build version is `0.1.0-2.3.0`. Set the library version part with `-PlibraryVersion=1.2.3`.

The release workflow creates and uploads a signed Central bundle when you push a version tag. Before release, configure the repository secrets `SIGNING_KEY`, `SIGNING_PASSWORD`, and `CENTRAL_TOKEN`; the Central token must be the base64 form of `username:password`. The `io.github.gliter` namespace must be verified in Sonatype Central. The tag must match the project version with a `v` prefix, for example `v1.2.3-2.3.0`. The workflow uses the tag to select the release version, so do not push a tag that does not match the build version.

To create the signed bundle locally, provide the Gradle properties `signingKey` and `signingPassword`, or their matching environment variables. Then run:

```shell
./gradlew centralBundle
```
