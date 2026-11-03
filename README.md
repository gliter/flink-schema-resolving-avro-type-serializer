# Schema-Resolving Avro Serializer for Flink

[![Build](https://github.com/gliter/flink-schema-resolving-avro-type-serializer/actions/workflows/build.yml/badge.svg)](https://github.com/gliter/flink-schema-resolving-avro-type-serializer/actions/workflows/build.yml)

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
    implementation("io.github.gliter:flink-schema-resolving-avro-type-serializer:0.1.0-2.3.0")
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

The serializer uses the runtime schema fixed at startup and resolves its Schema Registry ID under the configured subject. Depending on `auto.register.schemas`, it registers the schema or requires it to exist already. It then writes the frame magic, a format version byte, the schema ID as a variable-length integer, and the Avro binary datum. The ID points to the exact writer schema for that value. New state defaults to empty magic; with a small Registry ID, the frame adds two bytes per value. The snapshot stores the magic length and bytes once; each value repeats only the magic bytes, which add no overhead when empty. Null is rejected, as with Flink's regular Avro serializer: Flink clears `ValueState` instead of storing null, rejects null list elements, and flags null map values itself.

The subject controls schema registration and lookup during writes. It is not used to choose a schema when reading. This means that changing the configured subject does not change how existing values are decoded, as long as their schema IDs can still be resolved by the Registry.

### Example frame bytes

Assume the writer schema is a record with one string field, `name`, the value is `{"name":"Ada"}`, and Schema Registry assigns that schema ID `42`.

New state uses empty magic by default:

```text
| Serializer header | Regular Avro binary datum        |
| 01      2A        | 06             41 64 61          |
| version schema ID | string length  UTF-8 for "Ada"   |
```

The bytes `06 41 64 61` are the complete regular Avro binary datum for this record. The serializer adds only the header before them.

The wire-format version is `1` (`01`). Schema ID `42` is an unsigned varint (`2A`). Avro encodes the string's byte length, `3`, as a zigzag varint (`06`), followed by its UTF-8 bytes. There is no record header or overall value-length prefix. This complete value takes six bytes.

When restoring legacy Flink Avro state with this example schema, automatic selection chooses the one-byte magic `FF`. The first field is a string: Flink encodes its length as a non-negative, four-byte big-endian integer, whose first byte cannot exceed `7F`. A legacy value therefore cannot start with `FF`, so one byte is enough to distinguish framed values without reading past a legacy value. The same newly written value then looks like this:

```text
| Serializer header        | Regular Avro binary datum        |
| FF     01      2A         | 06             41 64 61          |
| magic  version schema ID  | string length  UTF-8 for "Ada"   |
```

The snapshot stores the selected magic's length and bytes separately; each value contains only the magic bytes themselves. Empty magic therefore adds no bytes to a value. Schema IDs above `127` take multiple varint bytes; for example, ID `300` is `AC 02`.

For comparison, the original raw legacy value written by Flink's regular Avro serializer for this schema has no magic, version or schema ID, and uses a four-byte string length:

```text
00 00 00 03       41 64 61
string length    UTF-8 bytes for "Ada"
```

### When Flink reads a value

When every legacy value is at least as long as the magic, the serializer reads the entire magic into a buffer. Otherwise, it probes one byte at a time to avoid consuming bytes from the next value. This minimum-size analysis runs when bridging legacy state; the resulting flag is persisted in the snapshot and reused on subsequent restores. If every byte matches, the value is schema-resolving: it reads the schema ID and asks Schema Registry for the corresponding writer schema. If a byte does not match, the bytes read so far are replayed and the value is decoded as a raw legacy Avro value (see "Use with existing Flink Avro state"). It then decodes the Avro datum with both schemas:

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

### How legacy values are told apart

A raw legacy value has no header, so the serializer must recognise its own frame magic without ever mistaking a legacy value for it (a collision) or reading past the end of a short legacy value (an over-read). Flink's regular Avro serializer does not use Avro binary encoding: it writes int, float, enum and union indexes as 4 big-endian bytes, long and double as 8, strings and bytes with a 4-byte big-endian length, and only array and map item counts as varints. At restore time the serializer derives an all-`FF` magic directly from the legacy writer schema. For an explicit override or unsafe fallback, a byte-level analysis checks possible collisions and over-reads. The selected magic and the reason for the choice are logged:

- The schema determines the required all-`FF` prefix directly, up to `max.magic.length` bytes (eight by default). Fixed-width numeric fields advance the prefix offset; the first constrained field determines its length. A legacy value that starts with a string, bytes, union, enum or boolean field can never begin with a byte above `0x7F`, so for those schemas the one-byte magic `0xFF` is proven exact and legacy values of any length, even a single byte, are read correctly. An array or map first field needs five bytes, because its varint count cannot carry five continuation bytes.
- If no candidate can be proven, for example when the value starts with a long followed by a double, with `use.unsafe.magic=true` the serializer falls back to the six-byte magic `0x53524156524F` (`SRAVRO`), provided no legacy value can be a proper prefix of it, and logs that detection relies on a legacy value not beginning with those six bytes by coincidence.
- If even that can over-read, for example a record with a single int field, or a schema that can encode to zero bytes, no magic is safe. Restore returns incompatible unless `allow.full.migration=true` requests a full migration.

Set `frame.magic` in `serializerConfig` to a hex string to choose the magic yourself. An explicit magic must be proven safe by default. When no proven safe magic is available, restore returns incompatible and logs two options: `use.unsafe.magic=true` accepts collision risk (but never over-read), or `allow.full.migration=true` forces a full rewrite using the old serializer. If both options are enabled, an accepted unsafe magic allows the lazy transition; otherwise full migration is permitted. The chosen magic is stored in the serializer snapshot and carried across later restores, so later jobs do not need to configure it. Changing `frame.magic` for state that already holds framed values forces a full migration.

Both options belong in `serializerConfig` and default to `false`. For example, to require a full rewrite whenever the selected legacy magic cannot be proven safe:

```java
Map<String, Object> serializerConfig = Map.of(
    "schema.registry.url", "https://schema-registry.example",
    "allow.full.migration", true);
```

Alternatively, set `"use.unsafe.magic", true` to allow the unsafe fallback. This accepts a risk of misreading a legacy value whose bytes happen to match the magic; it never allows a magic that can read past the value. If no acceptable magic exists, restore returns incompatible unless `allow.full.migration=true` permits a full rewrite.

The migration policy applies when taking over regular Flink Avro state and the chosen magic is not proven safe. Flink reads all old values with its original Avro serializer and writes them with the current serializer, leaving no raw legacy values. With no explicit `frame.magic`, these rewritten values use empty magic. If a proven-safe magic is available, this policy keeps the lazy transition; use `force.migration.nonce` to request a rewrite regardless of magic safety. After a successful migration, later snapshots describe framed state, so leaving the policy enabled does not trigger repeated rewrites.

### Restore scenarios

Options below are set in `serializerConfig`. Unless stated otherwise, options are unset: `allow.full.migration=false`, `use.unsafe.magic=false`, and no migration nonce or explicit magic. Restore rows assume Avro compatibility verification succeeds; the final rows describe verification failures.

| Situation | Options set | What happens |
| --- | --- | --- |
| Fresh state | Defaults | Writes with empty magic, followed by version, schema ID and regular Avro bytes. No legacy schema is stored. |
| Fresh state | `frame.magic="AB"` | Writes with the explicit magic and stores it in the snapshot. No rewrite is needed. |
| Fresh state | `allow.full.migration=true` | Same as the default fresh-state case; permission alone does not trigger migration. |
| Legacy Flink Avro state; a proven-safe automatic magic exists | Defaults | Lazy transition: selects magic from the legacy schema, preserves that schema in the snapshot, and allows old raw and new framed values to coexist. |
| Legacy state; a proven-safe explicit magic is supplied | `max.magic.length` | `8` | Maximum automatically selected magic length, including the unsafe fallback; integer from 1 to 16. Does not limit explicit `frame.magic` or change magic preserved in existing snapshots. If no accepted magic fits, normal incompatibility/full-migration rules apply. |
| `frame.magic` | Lazy transition using the explicit magic. |
| Legacy state; no proven-safe selected magic exists | Defaults | Returns incompatible unless `allow.full.migration=true` permits a full rewrite using the original Flink Avro reader. The rewrite drops the legacy schema and uses empty magic unless explicitly configured. Logs the unsafe-magic and full-migration options. |
| Legacy state; unsafe fallback or explicit magic can collide but cannot over-read | `use.unsafe.magic=true` | Lazy transition accepting collision risk. Uses the explicit magic, or the `SRAVRO` fallback when no safe automatic magic is found. |
| Legacy state; unsafe magic is requested | `use.unsafe.magic=true` | Allows a lazy transition with collision risk, but never over-read. If the selected magic can over-read, returns incompatible unless `allow.full.migration=true` permits a full rewrite instead. |
| Existing framed state, possibly mixed with raw legacy values | Same magic, or `frame.magic` removed; no new nonce | Preserves the snapshot's magic and any legacy schema. Removing the explicit option does not reset magic to empty. No full rewrite. |
| Existing framed state | Different explicit `frame.magic` | Returns incompatible unless `allow.full.migration=true`. When enabled, fully rewrites using the old reader and new magic, dropping any legacy schema. No nonce change is needed. |
| Supported legacy or framed state | New `force.migration.nonce` | Returns incompatible unless `allow.full.migration=true`. When enabled, fully rewrites to the current schema and configured magic (empty by default), drops the legacy schema, and saves the nonce in the next snapshot. |
| Framed state after a completed nonce-triggered migration | Same nonce | Does not repeat the rewrite; ordinary magic compatibility rules still apply. |
| Reader schema cannot read a checked legacy or registered writer schema | Verification enabled (default) | Returns incompatible. Migration permission does not override Avro incompatibility. |
| Reader schema compatibility is not checked | `verify.schema.compatibility=false` | Skips verification; magic and migration rules still apply. Unreadable values can fail during decoding. |
| Registry access or reader-schema registration/lookup fails during verification | Verification enabled (default) | Restore fails with an error. Migration permission does not bypass Registry failures. |
| Previous snapshot uses an unsupported serializer | Any options | Returns incompatible; these options do not enable arbitrary serializer migrations. |

## Force a full migration

Full state rewrites require `allow.full.migration=true` in `serializerConfig`; it defaults to `false`. This gate applies to nonce changes, explicit magic changes, and legacy restores with no accepted magic. Without permission, a restore requiring a rewrite returns incompatible and logs that enabling `allow.full.migration` permits migration. When legacy magic cannot be accepted, permission allows a rewrite; otherwise permission alone does not request one, and it does not override incompatible Avro schemas. Lazy restores do not require it.

Lazy schema resolution avoids a full rewrite, but some operational changes may require all stored values to use the current schema and wire format. For example, you may want to finish the transition from regular Flink Avro state or remove old schema versions from active state. In that case, request a full migration by setting `force.migration.nonce` to an integer in `serializerConfig`:

```java
Map<String, Object> migrationConfig = Map.of(
    "auto.register.schemas", true,
    "force.migration.nonce", 1,
    "allow.full.migration", true);
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
| `allow.full.migration` | `false` | Permit full rewrites requested by nonce changes, magic changes or the legacy unsafe-magic migration policy. Otherwise return incompatible with a diagnostic. |
| `use.unsafe.magic` | `false` | Allow potentially colliding legacy magic, provided it cannot over-read. |
| `max.magic.length` | `8` | Maximum automatically selected magic length, including the unsafe fallback; integer from 1 to 16. Does not limit explicit `frame.magic` or change magic preserved in existing snapshots. If no accepted magic fits, normal incompatibility/full-migration rules apply. |
| `frame.magic` | Chosen automatically | Hex bytes that start every framed value, for example `FF` or `53524156524F`. Unset, the serializer uses empty magic for new state and picks a magic from the legacy writer schema when taking over regular Flink Avro state. Changing it for existing framed state forces a full migration. |
| `verify.schema.compatibility` | `true` | At restore time, check that the current reader schema can read every recorded writer schema potentially present in state and any legacy raw-Avro writer schema, and register the reader schema. Restore fails fast on an incompatibility instead of failing later when a value is read. Set to `false` to skip the check. |

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

Schema evolution still follows Avro's writer/reader compatibility rules; the serializer does not repair incompatible schemas. A field added to the reader schema usually needs a default to read values written before that field existed. Because state can hold values written under any earlier schema version, the serializer checks at restore time that the reader schema can read every recorded writer schema potentially present in state and fails the restore otherwise (`verify.schema.compatibility`). Registry compatibility policy is independent: `BACKWARD_TRANSITIVE` can still reject a schema against versions removed from state by a full rewrite. Choose that policy according to the subject's other consumers as well as this serializer.

The legacy bridge supports one raw-Avro writer schema from the previous Flink serializer snapshot. New schema-resolving values do not have this limit because each value stores a Registry schema ID. Schema IDs must remain resolvable in the Registry used by the running job. The client may serve a schema from its cache, but a cache miss needs a Registry request, so the Flink processes need network access and valid credentials.

The configured subject is used when writing and registering schemas. A read uses the ID stored in the value, so it does not need the subject that was used to write that value. Normal schema resolution does not trigger a state rewrite; changing the force-migration nonce does.

## Development

The default build uses Java 17 and Flink 2.3.0. The build can also target Java 21 and Flink 2.2.1. Run the checks for the defaults with:

```shell
./gradlew check
./gradlew check -PjavaVersion=21 '-PflinkVersion=2.2.1'
```

GitHub Actions runs the test matrix for Java 17 and 21 with Flink 2.2.1 and 2.3.0. Unit tests cover serializer framing, null rejection, copying, restore-time schema compatibility checks, legacy raw-Avro reads, and frame magic selection from legacy schemas. RocksDB-backed Flink MiniCluster tests create and restore savepoints for `GenericRecord` and generated `SpecificRecord` values. These tests exercise state restore through Flink, not only direct calls to the serializer.

## Publishing

The Maven Central coordinates are `io.github.gliter:flink-schema-resolving-avro-type-serializer`. Published versions use the format `major.minor.patch-<Flink version>`, which makes the target Flink line visible in the version. Local builds use the same format with a `-SNAPSHOT` suffix. The default local build version is `1.0.0-2.3.0-SNAPSHOT`. Set the local library version part with `-PlibraryVersion=1.2.3`.

The release workflow takes the library version from a tag such as `v1.2.3`; it does not compare the tag with the local development version. It builds and uploads a separate non-SNAPSHOT artifact for each supported Flink version (for example, `1.2.3-2.2.1` and `1.2.3-2.3.0`). It creates a GitHub Release with generated notes after Central accepts both uploads; Central validation and publication continue asynchronously, so the artifacts may not yet be available in Maven Central when the GitHub Release appears. Before release, configure the repository secrets `SIGNING_KEY`, `SIGNING_PASSWORD`, and `CENTRAL_TOKEN`; the Central token must be the base64 form of `username:password`. The `io.github.gliter` namespace must be verified in Sonatype Central.

To create a signed release bundle locally, provide the Gradle properties `signingKey` and `signingPassword`, or their matching environment variables. Set `releaseBuild=true` to omit `-SNAPSHOT` from the version:

```shell
./gradlew centralBundle -PlibraryVersion=1.2.3 -PreleaseBuild=true -PflinkVersion=2.3.0
```

Writer schema history is stored as `SchemaInfo(subject, version, schemaId)` entries. Initialization records the configured runtime schema; lazy restores preserve all previous entries, including previous subjects. Compatibility checks resolve recorded IDs rather than scanning Registry history. Full rewrites reset the list to the current writer. DEBUG logs list entries; INFO logs show distinct candidate-version counts per subject. Callers must supply GenericRecords using the configured schema; normal writes do not compare schemas per record; forced migration projects historical records before writing.
